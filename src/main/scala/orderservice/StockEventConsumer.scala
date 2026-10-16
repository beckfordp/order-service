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

/** Consumes `StockReserved`/`StockReservationFailed` envelopes on
  * `inventory-events` (US-5.2; migrated to the domain topic + envelope shape
  * per ADR 0009) and updates the owning order's status via
  * `OrderStore.updateStatusByItemId`, keyed by each event's `orderItemId`. A
  * single correctly-attributed event is sufficient to flip an order's status
  *   - US-4.2's synchronous reserve call already verified every item succeeded
  *     (or failed) before leaving the order `Pending`, so this consumer is
  *     confirming already-known-good outcomes, not discovering new ones. No
  *     match (unknown item id, or the order already left `Pending`) is an
  *     idempotent no-op, logged at info - never an error, per spec.md's "Out of
  *     Scope". `inventory-events` may carry other event types this consumer
  *     doesn't care about - those are silently skipped by `eventType`, not
  *     logged as malformed.
  */
object StockEventConsumer {

  val topic: String = "inventory-events"

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
      .withGroupId("order-service-inventory-events")
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

  private def handleReserved[F[_]: Async](
      store: OrderStore[F],
      logger: StructuredLogger[F],
      raw: String,
      payload: io.circe.Json
  ): F[Unit] =
    payload.as[StockReservedEvent] match {
      case Left(error) =>
        logger.error(Map("raw" -> raw), error)(
          "Failed to decode StockReserved payload"
        )
      case Right(event) =>
        Async[F].realTimeInstant.flatMap { now =>
          store
            .updateStatusByItemId(
              event.orderItemId,
              OrderStatus.Reserved,
              Some(
                PendingOutboxEvent(
                  "OrderReserved",
                  ref =>
                    OrderReservedEvent(
                      ref.orderId,
                      ref.customerId,
                      ref.totalCents,
                      now
                    ).asJson
                )
              )
            )
            .flatMap(
              logOutcome(logger, event.orderItemId, OrderStatus.Reserved, _)
            )
        }
    }

  private def handleReservationFailed[F[_]: Async](
      store: OrderStore[F],
      logger: StructuredLogger[F],
      raw: String,
      payload: io.circe.Json
  ): F[Unit] =
    payload.as[StockReservationFailedEvent] match {
      case Left(error) =>
        logger.error(Map("raw" -> raw), error)(
          "Failed to decode StockReservationFailed payload"
        )
      case Right(event) =>
        Async[F].realTimeInstant.flatMap { now =>
          store
            .updateStatusByItemId(
              event.orderItemId,
              OrderStatus.ReservationFailed,
              Some(
                PendingOutboxEvent(
                  "OrderStatusChanged",
                  ref =>
                    OrderStatusChangedEvent(
                      ref.orderId,
                      ref.customerId,
                      OrderStatus.ReservationFailed.asString,
                      now
                    ).asJson
                )
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
    }

  def run[F[_]: Async](
      config: KafkaConfig,
      store: OrderStore[F],
      logger: StructuredLogger[F]
  ): Stream[F, Unit] =
    KafkaConsumer
      .stream(consumerSettings[F](config))
      .subscribeTo(topic)
      .records
      .evalMap { committable =>
        val handled: F[Unit] = committable.record.value match {
          case None =>
            logger.error(Map.empty)(
              "Received inventory-events record with a null value - skipping"
            )
          case Some(raw) =>
            decode[EventEnvelope](raw) match {
              case Left(error) =>
                logger.error(Map("raw" -> raw), error)(
                  "Failed to decode inventory-events envelope"
                )
              case Right(envelope) =>
                envelope.eventType match {
                  case "StockReserved" =>
                    handleReserved(store, logger, raw, envelope.payload)
                  case "StockReservationFailed" =>
                    handleReservationFailed(
                      store,
                      logger,
                      raw,
                      envelope.payload
                    )
                  case _ => Async[F].unit
                }
            }
        }
        handled *> committable.offset.commit
      }
}
