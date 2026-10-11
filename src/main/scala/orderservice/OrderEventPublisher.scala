package orderservice

import cats.Applicative
import cats.effect.{Async, Resource, Sync}
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.kafka._
import io.circe.syntax._
import org.typelevel.log4cats.StructuredLogger
import retry.{ResultHandler, RetryPolicies}

import java.util.UUID
import scala.concurrent.duration._

trait OrderEventPublisher[F[_]] {
  def publishReserved(event: OrderReservedEvent): F[Unit]
  def publishStatusChanged(event: OrderStatusChangedEvent): F[Unit]
}

object OrderEventPublisher {

  val topic: String = "order-events"
  private val source: String = "order-service"

  /** Bounds how long a single publish attempt can take. */
  private val publishTimeout: FiniteDuration = 2.seconds

  /** No transactional outbox (decided in `gluon/docs/system-design.md`):
    * bounded retry, then log loudly and drop - same risk level
    * inventory-service's publisher and payment-service's
    * `PaymentEventPublisher` already accept. `maxRetries` is retries, not
    * attempts (3 retries = 4 total attempts).
    */
  private val maxRetries = 3
  private val baseDelay: FiniteDuration = 100.millis

  /** For call sites that don't care about Kafka at all. */
  def noOp[F[_]: Applicative]: OrderEventPublisher[F] =
    new OrderEventPublisher[F] {
      def publishReserved(event: OrderReservedEvent): F[Unit] =
        Applicative[F].unit
      def publishStatusChanged(event: OrderStatusChangedEvent): F[Unit] =
        Applicative[F].unit
    }

  def resource[F[_]: Async](
      config: KafkaConfig,
      logger: StructuredLogger[F]
  ): Resource[F, OrderEventPublisher[F]] = {
    // The Kafka Java client's own defaults (max.block.ms/request.timeout.ms =
    // 60s) would block an unreachable-broker send far longer than
    // `publishTimeout` below can actually bound - a cats-effect `.timeout`
    // can't interrupt that underlying blocking call once it's started. These
    // properties make the client itself give up within `publishTimeout`, so
    // the bounded retry below actually bounds wall-clock time as documented.
    // (Same fix payment-service's PaymentEventPublisher needed for the same
    // reason.)
    val producerSettings =
      ProducerSettings[F, String, String]
        .withBootstrapServers(config.bootstrapServers)
        .withProperties(
          "max.block.ms" -> publishTimeout.toMillis.toString,
          "request.timeout.ms" -> publishTimeout.toMillis.toString,
          "delivery.timeout.ms" -> (publishTimeout.toMillis + 1000).toString
        )

    KafkaProducer.resource(producerSettings).map { producer =>
      new OrderEventPublisher[F] {
        private val policy =
          RetryPolicies
            .limitRetries[F](maxRetries)
            .join(RetryPolicies.exponentialBackoff[F](baseDelay))

        private def publish(key: String, envelope: EventEnvelope): F[Unit] = {
          val attempt =
            producer
              .produceOne_(ProducerRecord(topic, key, envelope.asJson.noSpaces))
              .flatten
              .timeout(publishTimeout)
              .void

          retry
            .retryingOnErrors(attempt)(
              policy,
              ResultHandler.retryOnAllErrors[F, Unit] { (error, details) =>
                logger.warn(error)(
                  s"Publish to $topic failed (attempt ${details.retriesSoFar + 1}), retrying"
                )
              }
            )
            .attempt
            .flatMap {
              case Right(_)    => Applicative[F].unit
              case Left(error) =>
                logger.warn(error)(
                  s"Publish to $topic failed after retries exhausted, dropping event"
                )
            }
        }

        private def envelopeFor[A: io.circe.Encoder](
            eventType: String,
            correlationId: String,
            timestamp: java.time.Instant,
            payload: A
        ): F[EventEnvelope] =
          Sync[F].delay(UUID.randomUUID().toString).map { eventId =>
            EventEnvelope(
              eventId = eventId,
              eventType = eventType,
              timestamp = timestamp,
              source = source,
              correlationId = correlationId,
              payload = payload.asJson
            )
          }

        def publishReserved(event: OrderReservedEvent): F[Unit] =
          envelopeFor("OrderReserved", event.orderId, event.timestamp, event)
            .flatMap(publish(event.orderId, _))

        def publishStatusChanged(event: OrderStatusChangedEvent): F[Unit] =
          envelopeFor(
            "OrderStatusChanged",
            event.orderId,
            event.timestamp,
            event
          ).flatMap(publish(event.orderId, _))
      }
    }
  }
}
