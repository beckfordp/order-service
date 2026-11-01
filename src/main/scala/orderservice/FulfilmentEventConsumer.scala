package orderservice

import cats.effect.Async
import cats.effect.kernel.MonadCancelThrow
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.circe.parser.decode
import orderservice.domain.{OrderEvent, OrderId, TransitionRejected}
import org.typelevel.log4cats.StructuredLogger
import purekafka.consumer.DeadLetter
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample}

/** Local mirror of fulfillment-service's `DispatchConfirmed` payload
  * (`gluon/docs/system-design.md`). Items are a full snapshot but this service
  * only needs the orderId; they are decoded to keep the contract checked.
  *
  * Both outcomes apply through `OrderStore.transition` / `OrderEvent`, the same
  * pure lifecycle every other status change uses. A failed inventory release on
  * `DispatchFailed` records a reconciliation flag instead of only logging.
  */
final case class FulfilmentItem(
    @description("Product SKU.")
    @encodedExample("SKU-0042")
    sku: String,
    @description("Product name at dispatch time.")
    @encodedExample("Espresso cup")
    productName: String,
    @description("Units dispatched.")
    @encodedExample(2)
    quantity: Int,
    @description("Unit price, in cents.")
    @encodedExample(1250)
    unitPriceCents: Int
)

object FulfilmentItem {
  implicit val codec: Codec[FulfilmentItem] = deriveCodec
  implicit val schema: Schema[FulfilmentItem] =
    Schema.derived[FulfilmentItem].copy(name = None)
}

@description(
  "Order dispatched: pick/pack/dispatch succeeded. Carries a full item snapshot so consumers need no order lookup."
)
final case class DispatchConfirmedEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Carrier tracking number.")
    @encodedExample("TRK-123456789")
    trackingNo: String,
    @description("Items dispatched. Always sent.")
    items: List[FulfilmentItem]
)

object DispatchConfirmedEvent {
  implicit val codec: Codec[DispatchConfirmedEvent] = deriveCodec
  implicit val schema: Schema[DispatchConfirmedEvent] = Schema.derived
}

@description(
  "Dispatch failed: pick/pack/dispatch hit an error. payment-service voids the hold; order-service marks the order failed."
)
final case class DispatchFailedEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Why dispatch failed.")
    @encodedExample("carrier_unavailable")
    reason: String
)

object DispatchFailedEvent {
  implicit val codec: Codec[DispatchFailedEvent] = deriveCodec
  implicit val schema: Schema[DispatchFailedEvent] = Schema.derived
}

/** Consumes `DispatchConfirmed` on `fulfilment-events` (US-17.4): moves the
  * order from `authorized` to `dispatched`, once. Unknown or non-authorized
  * orders are a normal no-op (logged, offset committed). Malformed
  * envelopes/payloads are logged and dropped; processing is DLQ-guarded.
  * `DispatchFailed` is deliberately not handled here yet (US-18.3).
  */
object FulfilmentEventConsumer {

  val topic: String = "fulfilment-events"

  private def consumerSettings[F[_]: Async](
      config: KafkaConfig
  ): ConsumerSettings[F, Option[String], Option[String]] =
    ConsumerSettings[F, Option[String], Option[String]]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId("order-service-fulfilment-events")
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  def run[F[_]: Async](
      config: KafkaConfig,
      store: OrderStore[F],
      inventoryClient: InventoryClient[F],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, String]
  ): Stream[F, Unit] =
    KafkaConsumer
      .stream(consumerSettings[F](config))
      .subscribeTo(topic)
      .records
      .evalMap { committable =>
        def handleConfirmed(event: DispatchConfirmedEvent)(implicit
            F: MonadCancelThrow[F]
        ): F[Unit] =
          OrderId.parse(event.orderId) match {
            case Left(error) =>
              logger.error(
                Map("order_id" -> event.orderId, "error" -> error)
              )("Invalid orderId in DispatchConfirmed payload")
            case Right(orderId) =>
              store
                .transition(orderId, OrderEvent.DispatchConfirmed)
                .flatMap {
                  case Right(_) =>
                    logger.info(
                      Map(
                        "order_id" -> event.orderId,
                        "tracking_no" -> event.trackingNo
                      )
                    )("Order dispatched")
                  case Left(TransitionRejected.UnknownOrder) =>
                    logger.warn(Map("order_id" -> event.orderId))(
                      "DispatchConfirmed for an unknown order - skipping"
                    )
                  case Left(
                        TransitionRejected.IllegalTransition(from, to)
                      ) =>
                    logger.warn(
                      Map(
                        "order_id" -> event.orderId,
                        "from" -> from.asString,
                        "to" -> to.asString
                      )
                    )(
                      "DispatchConfirmed rejected for the order's current status"
                    )
                }
          }

        def handleFailed(event: DispatchFailedEvent)(implicit
            F: MonadCancelThrow[F]
        ): F[Unit] =
          OrderId.parse(event.orderId) match {
            case Left(error) =>
              logger.error(
                Map("order_id" -> event.orderId, "error" -> error)
              )("Invalid orderId in DispatchFailed payload")
            case Right(orderId) =>
              store
                .transition(orderId, OrderEvent.DispatchFailed)
                .flatMap {
                  case Right(_) =>
                    inventoryClient
                      .release(OrderId.render(orderId))
                      .handleErrorWith { releaseError =>
                        store
                          .flagReconciliation(
                            orderId,
                            s"inventory release failed after DispatchFailed: ${releaseError.getMessage}"
                          )
                          .attempt
                          .flatMap {
                            case Left(flagError) =>
                              logger.error(
                                Map("order_id" -> event.orderId),
                                flagError
                              )(
                                "Inventory release failed AND the reconciliation flag could not be written"
                              )
                            case Right(_) =>
                              logger.warn(
                                Map("order_id" -> event.orderId),
                                releaseError
                              )(
                                "Releasing reservations for a dispatch failure failed - reconciliation flagged"
                              )
                          }
                      } *> logger.warn(
                      Map(
                        "order_id" -> event.orderId,
                        "reason" -> event.reason
                      )
                    )("Order dispatch failed")
                  case Left(TransitionRejected.UnknownOrder) =>
                    logger.warn(Map("order_id" -> event.orderId))(
                      "DispatchFailed for an unknown order - skipping"
                    )
                  case Left(
                        TransitionRejected.IllegalTransition(from, to)
                      ) =>
                    logger.warn(
                      Map(
                        "order_id" -> event.orderId,
                        "from" -> from.asString,
                        "to" -> to.asString
                      )
                    )("DispatchFailed rejected for the order's current status")
                }
          }

        val key = committable.record.key.getOrElse("")

        val handled: F[Unit] = committable.record.value match {
          case None =>
            logger.error(Map.empty)(
              "Received fulfilment-events record with a null value - skipping"
            )
          case Some(raw) =>
            decode[EventEnvelope](raw) match {
              case Left(error) =>
                logger.error(
                  Map("topic" -> topic, "bytes" -> raw.length.toString),
                  error
                )(
                  "Failed to decode fulfilment-events envelope"
                )
              case Right(envelope) =>
                envelope.eventType match {
                  case "DispatchConfirmed" =>
                    envelope.payload.as[DispatchConfirmedEvent] match {
                      case Left(error) =>
                        logger.error(
                          Map("topic" -> topic, "bytes" -> raw.length.toString),
                          error
                        )(
                          "Failed to decode DispatchConfirmed payload"
                        )
                      case Right(event) =>
                        deadLetter.guard(topic, key, raw)(
                          handleConfirmed(event)
                        )
                    }
                  case "DispatchFailed" =>
                    envelope.payload.as[DispatchFailedEvent] match {
                      case Left(error) =>
                        logger.error(
                          Map("topic" -> topic, "bytes" -> raw.length.toString),
                          error
                        )(
                          "Failed to decode DispatchFailed payload"
                        )
                      case Right(event) =>
                        deadLetter.guard(topic, key, raw)(
                          handleFailed(event)
                        )
                    }
                  case other =>
                    logger.info(
                      Map("topic" -> topic, "event_type" -> other)
                    )("Ignoring unrelated fulfilment-events event type")
                }
            }
        }
        handled *> KafkaCommit.commit(committable)
      }
}
