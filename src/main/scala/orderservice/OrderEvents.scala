package orderservice

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema

import java.time.Instant

/** Per-outcome order lifecycle events published on `order-events` via the
  * outbox/Debezium CDC connector (ADR 0008). Payload shapes are pinned in
  * `gluon/docs/system-design.md`'s "Payload contracts" section - that
  * cross-repo doc, not these case classes, is the source of truth.
  *
  * Split from a single `OrderStatusChanged {status}` event in TD-15, matching
  * the per-outcome style payment-service and fulfilment-service already use.
  * Dispatch outcomes stay authoritative on `fulfilment-events`
  * (`DispatchConfirmed`/`DispatchFailed`) and are deliberately not mirrored
  * here. Checkout publishes nothing: a failed checkout leaves no order
  * (ADR 0018).
  */
final case class OrderConfirmedEvent(
    orderId: String,
    customerId: String,
    timestamp: Instant
)

object OrderConfirmedEvent {
  implicit val codec: Codec[OrderConfirmedEvent] = deriveCodec
  implicit val schema: Schema[OrderConfirmedEvent] = Schema.derived
}

/** Emitted when order-service consumes the capture-failure `PaymentFailed`
  * and moves the order to `payment_failed`. `reason` carries payment-service's
  * capture-failure reason (e.g. `capture_failed`) for manual reconciliation.
  */
final case class OrderPaymentFailedEvent(
    orderId: String,
    customerId: String,
    reason: String,
    timestamp: Instant
)

object OrderPaymentFailedEvent {
  implicit val codec: Codec[OrderPaymentFailedEvent] = deriveCodec
  implicit val schema: Schema[OrderPaymentFailedEvent] = Schema.derived
}

/** Emitted when order-service consumes `PaymentAuthorizationExpired` and moves
  * the order to `cancelled` (Journey 5).
  */
final case class OrderCancelledEvent(
    orderId: String,
    customerId: String,
    timestamp: Instant
)

object OrderCancelledEvent {
  implicit val codec: Codec[OrderCancelledEvent] = deriveCodec
  implicit val schema: Schema[OrderCancelledEvent] = Schema.derived
}
