package orderservice

import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.circe.parser.decode
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

/** Consumes `payment.settled`/`payment.failed` (payment-service, US-6.1) and
  * transitions the matching order from `Reserved` to `Confirmed`/
  * `PaymentFailed` via `OrderStore.updateStatusIfCurrent`, keyed by the event's
  * `orderId` directly. A redelivered/out-of-order event for an order that's no
  * longer `Reserved` (or an unknown `orderId`) is an idempotent no-op, logged
  * at info - never an error, per spec.md's "Out of Scope". Publishes
  * `order.status-changed` on a successful transition, mirroring
  * `StockEventConsumer`'s precedent exactly.
  */
object PaymentEventConsumer {

  val settledTopic: String = "payment.settled"
  val failedTopic: String = "payment.failed"

  private def consumerSettings[F[_]: Async](
      config: KafkaConfig,
      groupId: String
  ): ConsumerSettings[F, String, String] =
    ConsumerSettings[F, String, String]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId(groupId)
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

  private def settledStream[F[_]: Async](
      config: KafkaConfig,
      store: OrderStore[F],
      logger: StructuredLogger[F],
      publisher: OrderEventPublisher[F]
  ): Stream[F, Unit] =
    KafkaConsumer
      .stream(consumerSettings[F](config, "order-service-payment-settled"))
      .subscribeTo(settledTopic)
      .records
      .evalMap { committable =>
        val handled: F[Unit] =
          decode[PaymentSettledEvent](committable.record.value) match {
            case Left(error) =>
              logger.error(
                Map("raw" -> committable.record.value),
                error
              )("Failed to decode payment.settled")
            case Right(event) =>
              store
                .updateStatusIfCurrent(
                  event.orderId,
                  OrderStatus.Reserved,
                  OrderStatus.Confirmed
                )
                .flatTap(
                  publishStatusChangedIfUpdated(
                    publisher,
                    OrderStatus.Confirmed.asString,
                    _
                  )
                )
                .flatMap(
                  logOutcome(logger, event.orderId, OrderStatus.Confirmed, _)
                )
          }
        handled *> committable.offset.commit
      }

  private def failedStream[F[_]: Async](
      config: KafkaConfig,
      store: OrderStore[F],
      logger: StructuredLogger[F],
      publisher: OrderEventPublisher[F]
  ): Stream[F, Unit] =
    KafkaConsumer
      .stream(consumerSettings[F](config, "order-service-payment-failed"))
      .subscribeTo(failedTopic)
      .records
      .evalMap { committable =>
        val handled: F[Unit] =
          decode[PaymentFailedEvent](committable.record.value) match {
            case Left(error) =>
              logger.error(
                Map("raw" -> committable.record.value),
                error
              )("Failed to decode payment.failed")
            case Right(event) =>
              store
                .updateStatusIfCurrent(
                  event.orderId,
                  OrderStatus.Reserved,
                  OrderStatus.PaymentFailed
                )
                .flatTap(
                  publishStatusChangedIfUpdated(
                    publisher,
                    OrderStatus.PaymentFailed.asString,
                    _
                  )
                )
                .flatMap(
                  logOutcome(
                    logger,
                    event.orderId,
                    OrderStatus.PaymentFailed,
                    _
                  )
                )
          }
        handled *> committable.offset.commit
      }

  def run[F[_]: Async](
      config: KafkaConfig,
      store: OrderStore[F],
      logger: StructuredLogger[F],
      publisher: OrderEventPublisher[F]
  ): Stream[F, Unit] =
    settledStream(config, store, logger, publisher)
      .merge(failedStream(config, store, logger, publisher))
}
