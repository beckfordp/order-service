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
}

final case class PaymentFailedEvent(
    orderId: String,
    paymentId: String,
    amountCents: Int,
    timestamp: Instant
)

object PaymentFailedEvent {
  implicit val codec: Codec[PaymentFailedEvent] = deriveCodec
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

  private def consumerSettings[F[_]: Async](
      config: KafkaConfig
  ): ConsumerSettings[F, String, String] =
    ConsumerSettings[F, String, String]
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

  private def publishStatusChangedIfUpdated[F[_]: Async](
      publisher: OrderEventPublisher[F],
      status: String,
      updated: Option[UpdatedOrderRef]
  ): F[Unit] =
    updated match {
      case None           => Async[F].unit
      case Some(orderRef) =>
        Async[F].realTimeInstant.flatMap { now =>
          publisher.publishStatusChanged(
            OrderStatusChangedEvent(
              orderRef.orderId,
              orderRef.customerId,
              status,
              now
            )
          )
        }
    }

  private def handleSettled[F[_]: Async](
      store: OrderStore[F],
      logger: StructuredLogger[F],
      publisher: OrderEventPublisher[F],
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
      publisher: OrderEventPublisher[F],
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
      publisher: OrderEventPublisher[F]
  ): Stream[F, Unit] =
    KafkaConsumer
      .stream(consumerSettings[F](config))
      .subscribeTo(topic)
      .records
      .evalMap { committable =>
        val raw = committable.record.value
        val handled: F[Unit] = decode[EventEnvelope](raw) match {
          case Left(error) =>
            logger.error(Map("raw" -> raw), error)(
              "Failed to decode payment-events envelope"
            )
          case Right(envelope) =>
            envelope.eventType match {
              case "PaymentSettled" =>
                handleSettled(store, logger, publisher, raw, envelope.payload)
              case "PaymentFailed" =>
                handleFailed(store, logger, publisher, raw, envelope.payload)
              case _ => Async[F].unit
            }
        }
        handled *> committable.offset.commit
      }
}
