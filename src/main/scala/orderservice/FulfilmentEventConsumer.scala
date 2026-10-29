package orderservice

import cats.effect.Async
import cats.effect.kernel.MonadCancelThrow
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.circe.parser.decode
import orderservice.domain.OrderId
import org.typelevel.log4cats.StructuredLogger
import purekafka.consumer.DeadLetter
import sttp.tapir.Schema

/** Local mirror of fulfillment-service's `DispatchConfirmed` payload
  * (`gluon/docs/system-design.md`). Items are a full snapshot but this service
  * only needs the orderId; they are decoded to keep the contract checked.
  */
final case class FulfilmentItem(
    sku: String,
    productName: String,
    quantity: Int,
    unitPriceCents: Int
)

object FulfilmentItem {
  implicit val codec: Codec[FulfilmentItem] = deriveCodec
  implicit val schema: Schema[FulfilmentItem] =
    Schema.derived[FulfilmentItem].copy(name = None)
}

final case class DispatchConfirmedEvent(
    orderId: String,
    trackingNo: String,
    items: List[FulfilmentItem]
)

object DispatchConfirmedEvent {
  implicit val codec: Codec[DispatchConfirmedEvent] = deriveCodec
  implicit val schema: Schema[DispatchConfirmedEvent] = Schema.derived
}

final case class DispatchFailedEvent(orderId: String, reason: String)

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
                .updateStatusIfCurrent(
                  orderId,
                  OrderStatus.Authorized,
                  OrderStatus.Dispatched
                )
                .flatMap {
                  case Some(_) =>
                    logger.info(
                      Map(
                        "order_id" -> event.orderId,
                        "tracking_no" -> event.trackingNo
                      )
                    )("Order dispatched")
                  case None =>
                    logger.warn(Map("order_id" -> event.orderId))(
                      "DispatchConfirmed for an unknown or non-authorized order - skipping"
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
                .updateStatusIfCurrent(
                  orderId,
                  OrderStatus.Authorized,
                  OrderStatus.DispatchFailed
                )
                .flatMap {
                  case Some(_) =>
                    inventoryClient
                      .release(OrderId.render(orderId))
                      .handleErrorWith { error =>
                        logger.warn(
                          Map("order_id" -> event.orderId),
                          error
                        )(
                          "Releasing reservations for a dispatch failure failed - reconciliation needed"
                        )
                      } *> logger.warn(
                      Map(
                        "order_id" -> event.orderId,
                        "reason" -> event.reason
                      )
                    )("Order dispatch failed")
                  case None =>
                    logger.warn(Map("order_id" -> event.orderId))(
                      "DispatchFailed for an unknown or non-authorized order - skipping"
                    )
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
        handled *> committerCommit(committable)
      }

  private def committerCommit[F[_]](
      committable: CommittableConsumerRecord[F, Option[String], Option[String]]
  ): F[Unit] = committable.offset.commit
}
