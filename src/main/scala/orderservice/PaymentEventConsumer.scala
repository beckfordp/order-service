package orderservice

import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.circe.parser.decode
import io.circe.syntax._
import org.typelevel.log4cats.StructuredLogger
import purekafka.consumer.DeadLetter
import sttp.tapir.Schema

import java.time.Instant

/** Local mirror of payment-service's published payload shape, pinned in
  * `gluon/docs/system-design.md`'s "Payload contracts" section - that
  * cross-repo doc, not payment-service's own case classes, is the source of
  * truth. No shared library between the two services; each side keeps its own
  * copy.
  */
final case class PaymentSettledEvent(
    orderId: String,
    paymentId: String,
    amountCents: Int,
    timestamp: Instant
)

object PaymentSettledEvent {
  implicit val codec: Codec[PaymentSettledEvent] = deriveCodec
  implicit val schema: Schema[PaymentSettledEvent] = Schema.derived
}

final case class PaymentFailedEvent(
    orderId: String,
    paymentId: String,
    amountCents: Int,
    timestamp: Instant
)

object PaymentFailedEvent {
  implicit val codec: Codec[PaymentFailedEvent] = deriveCodec
  implicit val schema: Schema[PaymentFailedEvent] = Schema.derived
}

/** Consumes `PaymentSettled`/`PaymentFailed` envelopes on `payment-events`
  * (payment-service, US-6.1; migrated to the domain topic + envelope shape per
  * ADR 0009) and transitions the matching order from `Reserved` to
  * `Confirmed`/`PaymentFailed` via `OrderStore.updateStatusIfCurrent`, keyed by
  * the event's `orderId` directly. A redelivered/out-of-order event for an
  * order that's no longer `Reserved` (or an unknown `orderId`) is an idempotent
  * no-op, logged at info - never an error, per spec.md's "Out of Scope".
  * Publishes `OrderStatusChanged` on a successful transition, mirroring
  * `StockEventConsumer`'s precedent exactly. `payment-events` may carry other
  * event types this consumer doesn't care about - those are silently skipped by
  * `eventType`, not logged as malformed.
  */
object PaymentEventConsumer {

  val topic: String = "payment-events"

  // Key/value typed as Option[String] (via fs2-kafka's null-safe
  // Deserializer.option) rather than plain String: a plain String
  // deserializer throws on a null key or value, which kills this whole
  // background consumer stream silently - see TD-2.2, ADR 0010. Mirrors
  // payment-service's OrderReservedConsumer, which fixed the identical
  // crash risk first (TD-2.1).
  private def consumerSettings[F[_]: Async](
      config: KafkaConfig
  ): ConsumerSettings[F, Option[String], Option[String]] =
    ConsumerSettings[F, Option[String], Option[String]]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId("order-service-payment-events")
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  private def logOutcome[F[_]: Async](
      logger: StructuredLogger[F],
      orderId: String,
      newStatus: OrderStatus,
      updated: Option[UpdatedOrderRef]
  ): F[Unit] =
    updated match {
      case Some(_) =>
        logger.info(
          Map(
            "order_id" -> orderId,
            "new_status" -> newStatus.asString
          )
        )("Order status updated from payment-settlement event")
      case None =>
        logger.info(
          Map(
            "order_id" -> orderId,
            "new_status" -> newStatus.asString
          )
        )(
          "No matching Reserved order for orderId (unknown order, or already resolved) - ignoring"
        )
    }

  private def handleSettled[F[_]: Async](
      store: OrderStore[F],
      logger: StructuredLogger[F],
      raw: String,
      payload: io.circe.Json
  ): F[Unit] =
    payload.as[PaymentSettledEvent] match {
      case Left(error) =>
        logger.error(Map("raw" -> raw), error)(
          "Failed to decode PaymentSettled payload"
        )
      case Right(event) =>
        Async[F].realTimeInstant.flatMap { now =>
          store
            .updateStatusIfCurrent(
              event.orderId,
              OrderStatus.Reserved,
              OrderStatus.Confirmed,
              Some(
                PendingOutboxEvent(
                  "OrderStatusChanged",
                  ref =>
                    OrderStatusChangedEvent(
                      ref.orderId,
                      ref.customerId,
                      OrderStatus.Confirmed.asString,
                      now
                    ).asJson
                )
              )
            )
            .flatMap(
              logOutcome(logger, event.orderId, OrderStatus.Confirmed, _)
            )
        }
    }

  private def handleFailed[F[_]: Async](
      store: OrderStore[F],
      logger: StructuredLogger[F],
      raw: String,
      payload: io.circe.Json
  ): F[Unit] =
    payload.as[PaymentFailedEvent] match {
      case Left(error) =>
        logger.error(Map("raw" -> raw), error)(
          "Failed to decode PaymentFailed payload"
        )
      case Right(event) =>
        Async[F].realTimeInstant.flatMap { now =>
          store
            .updateStatusIfCurrent(
              event.orderId,
              OrderStatus.Reserved,
              OrderStatus.PaymentFailed,
              Some(
                PendingOutboxEvent(
                  "OrderStatusChanged",
                  ref =>
                    OrderStatusChangedEvent(
                      ref.orderId,
                      ref.customerId,
                      OrderStatus.PaymentFailed.asString,
                      now
                    ).asJson
                )
              )
            )
            .flatMap(
              logOutcome(logger, event.orderId, OrderStatus.PaymentFailed, _)
            )
        }
    }

  def run[F[_]: Async](
      config: KafkaConfig,
      store: OrderStore[F],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, String]
  ): Stream[F, Unit] =
    KafkaConsumer
      .stream(consumerSettings[F](config))
      .subscribeTo(topic)
      .records
      .evalMap { committable =>
        val handled: F[Unit] = committable.record.value match {
          case None =>
            logger.error(Map.empty)(
              "Received payment-events record with a null value - skipping"
            )
          case Some(raw) =>
            decode[EventEnvelope](raw) match {
              case Left(error) =>
                logger.error(Map("raw" -> raw), error)(
                  "Failed to decode payment-events envelope"
                )
              case Right(envelope) =>
                // Decode failures above are a different, already-accepted
                // gap (logged and dropped, never retried) - only a
                // message that decodes fine but fails *processing* (e.g.
                // a transient store error) gets the bounded-retry-then-DLQ
                // treatment, via purekafka.consumer.DeadLetter (TD-2.4,
                // ADR 0010).
                val key = committable.record.key.getOrElse("")
                envelope.eventType match {
                  case "PaymentSettled" =>
                    deadLetter.guard(topic, key, raw)(
                      handleSettled(store, logger, raw, envelope.payload)
                    )
                  case "PaymentFailed" =>
                    deadLetter.guard(topic, key, raw)(
                      handleFailed(store, logger, raw, envelope.payload)
                    )
                  case _ => Async[F].unit
                }
            }
        }
        handled *> committable.offset.commit
      }
}
