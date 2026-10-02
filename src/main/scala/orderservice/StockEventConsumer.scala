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

/** Local mirrors of inventory-service's published payload shape, pinned in
  * `gluon/docs/system-design.md`'s "Payload contracts" section - that
  * cross-repo doc, not inventory-service's own case classes, is the source of
  * truth. No shared library between the two services; each side keeps its own
  * copy.
  */
final case class StockReservedEvent(
    orderItemId: String,
    sku: String,
    quantity: Int,
    timestamp: Instant
)

object StockReservedEvent {
  implicit val codec: Codec[StockReservedEvent] = deriveCodec
}

final case class StockReservationFailedEvent(
    orderItemId: String,
    sku: String,
    quantity: Int,
    timestamp: Instant
)

object StockReservationFailedEvent {
  implicit val codec: Codec[StockReservationFailedEvent] = deriveCodec
}

/** Consumes `inventory.stock-reserved`/`inventory.stock-reservation-failed`
  * (US-5.2) and updates the owning order's status via
  * `OrderStore.updateStatusByItemId`, keyed by each event's `orderItemId`. A
  * single correctly-attributed event is sufficient to flip an order's status
  *   - US-4.2's synchronous reserve call already verified every item succeeded
  *     (or failed) before leaving the order `Pending`, so this consumer is
  *     confirming already-known-good outcomes, not discovering new ones. No
  *     match (unknown item id, or the order already left `Pending`) is an
  *     idempotent no-op, logged at info - never an error, per spec.md's "Out of
  *     Scope".
  */
object StockEventConsumer {

  val reservedTopic: String = "inventory.stock-reserved"
  val reservationFailedTopic: String = "inventory.stock-reservation-failed"

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
      orderItemId: String,
      newStatus: OrderStatus,
      updated: Option[UpdatedOrderRef]
  ): F[Unit] =
    updated match {
      case Some(_) =>
        logger.info(
          Map(
            "order_item_id" -> orderItemId,
            "new_status" -> newStatus.asString
          )
        )("Order status updated from stock-reservation event")
      case None =>
        logger.info(
          Map(
            "order_item_id" -> orderItemId,
            "new_status" -> newStatus.asString
          )
        )(
          "No matching Pending order for orderItemId (unknown item, or already resolved) - ignoring"
        )
    }

  /** On a successful transition, publishes `order.reserved` with the order's
    * details - skipped entirely on the no-op case (unknown item id, or the
    * order already left `Pending`), since nothing changed to tell
    * payment-service about.
    */
  private def publishReservedIfUpdated[F[_]: Async](
      publisher: OrderEventPublisher[F],
      updated: Option[UpdatedOrderRef]
  ): F[Unit] =
    updated match {
      case None           => Async[F].unit
      case Some(orderRef) =>
        Async[F].realTimeInstant.flatMap { now =>
          publisher.publishReserved(
            OrderReservedEvent(
              orderRef.orderId,
              orderRef.customerId,
              orderRef.totalCents,
              now
            )
          )
        }
    }

  /** On a successful transition, publishes `order.status-changed` with the
    * given `status` - skipped entirely on the no-op case (unknown item id, or
    * the order already left `Pending`), since nothing changed to tell
    * notification-service about.
    */
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

  private def reservedStream[F[_]: Async](
      config: KafkaConfig,
      store: OrderStore[F],
      logger: StructuredLogger[F],
      publisher: OrderEventPublisher[F]
  ): Stream[F, Unit] =
    KafkaConsumer
      .stream(consumerSettings[F](config, "order-service-stock-reserved"))
      .subscribeTo(reservedTopic)
      .records
      .evalMap { committable =>
        val handled: F[Unit] =
          decode[StockReservedEvent](committable.record.value) match {
            case Left(error) =>
              logger.error(
                Map("raw" -> committable.record.value),
                error
              )("Failed to decode inventory.stock-reserved")
            case Right(event) =>
              store
                .updateStatusByItemId(event.orderItemId, OrderStatus.Reserved)
                .flatTap(publishReservedIfUpdated(publisher, _))
                .flatMap(
                  logOutcome(
                    logger,
                    event.orderItemId,
                    OrderStatus.Reserved,
                    _
                  )
                )
          }
        handled *> committable.offset.commit
      }

  private def reservationFailedStream[F[_]: Async](
      config: KafkaConfig,
      store: OrderStore[F],
      logger: StructuredLogger[F],
      publisher: OrderEventPublisher[F]
  ): Stream[F, Unit] =
    KafkaConsumer
      .stream(
        consumerSettings[F](config, "order-service-stock-reservation-failed")
      )
      .subscribeTo(reservationFailedTopic)
      .records
      .evalMap { committable =>
        val handled: F[Unit] =
          decode[StockReservationFailedEvent](
            committable.record.value
          ) match {
            case Left(error) =>
              logger.error(
                Map("raw" -> committable.record.value),
                error
              )("Failed to decode inventory.stock-reservation-failed")
            case Right(event) =>
              store
                .updateStatusByItemId(
                  event.orderItemId,
                  OrderStatus.ReservationFailed
                )
                .flatTap(
                  publishStatusChangedIfUpdated(
                    publisher,
                    OrderStatus.ReservationFailed.asString,
                    _
                  )
                )
                .flatMap(
                  logOutcome(
                    logger,
                    event.orderItemId,
                    OrderStatus.ReservationFailed,
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
    reservedStream(config, store, logger, publisher)
      .merge(reservationFailedStream(config, store, logger, publisher))
}
