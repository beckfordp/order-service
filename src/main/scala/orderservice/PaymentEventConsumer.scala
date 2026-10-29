package orderservice

import cats.Monad
import cats.effect.{Async, Clock}
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.circe.parser.decode
import io.circe.syntax._
import orderservice.domain.{OrderEvent, OrderId, TransitionRejected}
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
final case class PaymentCapturedEvent(
    orderId: String,
    amountCents: Int,
    timestamp: Instant
)

object PaymentCapturedEvent {
  implicit val codec: Codec[PaymentCapturedEvent] = deriveCodec
  implicit val schema: Schema[PaymentCapturedEvent] = Schema.derived
}

final case class PaymentAuthorizationExpiredEvent(
    orderId: String,
    timestamp: Instant
)

object PaymentAuthorizationExpiredEvent {
  implicit val codec: Codec[PaymentAuthorizationExpiredEvent] = deriveCodec
  implicit val schema: Schema[PaymentAuthorizationExpiredEvent] =
    Schema.derived
}

final case class PaymentFailedEvent(
    orderId: String,
    paymentId: String,
    amountCents: Int,
    reason: String,
    timestamp: Instant
)

object PaymentFailedEvent {
  implicit val codec: Codec[PaymentFailedEvent] = deriveCodec
  implicit val schema: Schema[PaymentFailedEvent] = Schema.derived
}

/** Consumes `PaymentCaptured`/`PaymentAuthorizationExpired`/`PaymentFailed`
  * envelopes on `payment-events` (payment-service; authorization is now the
  * synchronous `POST /payments/authorize` call, so this consumer never sees
  * `PaymentAuthorized` - order-service already has that outcome).
  *
  * Capture outcomes use `OrderStore.recordCaptureOutcome`, which accepts an
  * order still `Authorized` or already `Dispatched`: this consumer and the
  * fulfilment consumer are independent streams, so `PaymentCaptured` can
  * overtake `DispatchConfirmed`. `PaymentCaptured -> Confirmed`;
  * `PaymentFailed -> PaymentFailed` with a manual-reconciliation flag and
  * deliberately **no** inventory release (US-19.3 - the goods may already be
  * shipped). There is no `InventoryClient` dependency here at all, which
  * enforces that rule structurally. Expiry stays `Authorized -> Cancelled`
  * (Journey 5).
  *
  * A redelivered/out-of-order event for an order no longer awaiting capture (or
  * an unknown `orderId`) is an idempotent no-op, logged at info - never an
  * error, per spec.md's "Out of Scope". Publishes the matching per-outcome
  * order event (`OrderConfirmed`/`OrderPaymentFailed`/`OrderCancelled`) on a
  * successful transition. `payment-events` may carry other event types this
  * consumer doesn't care about - those are silently skipped by `eventType`, not
  * logged as malformed.
  */
object PaymentEventConsumer {

  val topic: String = "payment-events"

  // Key/value typed as Option[String] (via fs2-kafka's null-safe
  // Deserializer.option) rather than plain String: a plain String
  // deserializer throws on a null key or value, which kills this whole
  // background consumer stream silently - see TD-2.2, ADR 0010.
  private def consumerSettings[F[_]: Async](
      config: KafkaConfig
  ): ConsumerSettings[F, Option[String], Option[String]] =
    ConsumerSettings[F, Option[String], Option[String]]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId("order-service-payment-events")
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  private def logOutcome[F[_]: Monad](
      logger: StructuredLogger[F],
      orderId: String,
      eventLabel: String,
      updated: Either[TransitionRejected, UpdatedOrderRef]
  ): F[Unit] =
    updated match {
      case Right(_) =>
        logger.info(
          Map(
            "order_id" -> orderId,
            "event" -> eventLabel
          )
        )("Order status updated from payment event")
      case Left(TransitionRejected.UnknownOrder) =>
        logger.info(
          Map("order_id" -> orderId, "event" -> eventLabel)
        )("Unknown orderId - ignoring")
      case Left(TransitionRejected.IllegalTransition(from, to)) =>
        logger.info(
          Map(
            "order_id" -> orderId,
            "event" -> eventLabel,
            "from" -> from.asString,
            "to" -> to.asString
          )
        )("Order not awaiting this transition - ignoring")
    }

  private def handleCaptured[F[_]: Monad: Clock](
      store: OrderStore[F],
      logger: StructuredLogger[F],
      raw: String,
      payload: io.circe.Json
  ): F[Unit] =
    payload.as[PaymentCapturedEvent] match {
      case Left(error) =>
        logger.error(
          Map("topic" -> topic, "bytes" -> raw.length.toString),
          error
        )(
          "Failed to decode PaymentCaptured payload"
        )
      case Right(event) =>
        OrderId.parse(event.orderId) match {
          case Left(error) =>
            logger.error(
              Map("order_id" -> event.orderId, "error" -> error)
            )("Invalid orderId in PaymentCaptured payload")
          case Right(orderId) =>
            Clock[F].realTimeInstant.flatMap { now =>
              store
                .transition(
                  orderId,
                  OrderEvent.PaymentCaptured,
                  Some(
                    PendingOutboxEvent(
                      "OrderConfirmed",
                      ref =>
                        OrderConfirmedEvent(
                          OrderId.render(ref.orderId),
                          ref.customerId,
                          now
                        ).asJson
                    )
                  )
                )
                .flatMap(
                  logOutcome(
                    logger,
                    event.orderId,
                    "PaymentCaptured",
                    _
                  )
                )
            }
        }
    }

  private def handleExpired[F[_]: Monad: Clock](
      store: OrderStore[F],
      logger: StructuredLogger[F],
      raw: String,
      payload: io.circe.Json
  ): F[Unit] =
    payload.as[PaymentAuthorizationExpiredEvent] match {
      case Left(error) =>
        logger.error(
          Map("topic" -> topic, "bytes" -> raw.length.toString),
          error
        )(
          "Failed to decode PaymentAuthorizationExpired payload"
        )
      case Right(event) =>
        OrderId.parse(event.orderId) match {
          case Left(error) =>
            logger.error(
              Map("order_id" -> event.orderId, "error" -> error)
            )("Invalid orderId in PaymentAuthorizationExpired payload")
          case Right(orderId) =>
            Clock[F].realTimeInstant.flatMap { now =>
              store
                .transition(
                  orderId,
                  OrderEvent.AuthorizationExpired,
                  Some(
                    PendingOutboxEvent(
                      "OrderCancelled",
                      ref =>
                        OrderCancelledEvent(
                          OrderId.render(ref.orderId),
                          ref.customerId,
                          now
                        ).asJson
                    )
                  )
                )
                .flatMap(
                  logOutcome(
                    logger,
                    event.orderId,
                    "PaymentAuthorizationExpired",
                    _
                  )
                )
            }
        }
    }

  private def handleFailed[F[_]: Monad: Clock](
      store: OrderStore[F],
      logger: StructuredLogger[F],
      raw: String,
      payload: io.circe.Json
  ): F[Unit] =
    payload.as[PaymentFailedEvent] match {
      case Left(error) =>
        logger.error(
          Map("topic" -> topic, "bytes" -> raw.length.toString),
          error
        )(
          "Failed to decode PaymentFailed payload"
        )
      case Right(event) =>
        OrderId.parse(event.orderId) match {
          case Left(error) =>
            logger.error(
              Map("order_id" -> event.orderId, "error" -> error)
            )("Invalid orderId in PaymentFailed payload")
          case Right(orderId) =>
            Clock[F].realTimeInstant.flatMap { now =>
              store
                .transition(
                  orderId,
                  OrderEvent.PaymentFailed(event.reason),
                  Some(
                    PendingOutboxEvent(
                      "OrderPaymentFailed",
                      ref =>
                        OrderPaymentFailedEvent(
                          OrderId.render(ref.orderId),
                          ref.customerId,
                          event.reason,
                          now
                        ).asJson
                    )
                  ),
                  // US-19.3: flag for manual reconciliation, never auto-release
                  // inventory (the order may already be physically shipped).
                  reconciliationReason = Some(event.reason)
                )
                .flatMap(
                  logOutcome(
                    logger,
                    event.orderId,
                    "PaymentFailed",
                    _
                  )
                )
            }
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
                logger.error(
                  Map("topic" -> topic, "bytes" -> raw.length.toString),
                  error
                )(
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
                  case "PaymentCaptured" =>
                    deadLetter.guard(topic, key, raw)(
                      handleCaptured(store, logger, raw, envelope.payload)
                    )
                  case "PaymentAuthorizationExpired" =>
                    deadLetter.guard(topic, key, raw)(
                      handleExpired(store, logger, raw, envelope.payload)
                    )
                  case "PaymentFailed" =>
                    deadLetter.guard(topic, key, raw)(
                      handleFailed(store, logger, raw, envelope.payload)
                    )
                  case other =>
                    logger.info(
                      Map("topic" -> topic, "event_type" -> other)
                    )("Ignoring unrelated payment-events event type")
                }
            }
        }
        handled *> KafkaCommit.commit(committable)
      }
}
