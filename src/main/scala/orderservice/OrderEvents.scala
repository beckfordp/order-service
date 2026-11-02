package orderservice

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample}

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
  * here. Checkout publishes nothing: a failed checkout leaves no order (ADR
  * 0018).
  */
@description(
  "Order confirmed: order-service consumed PaymentCaptured and moved the order to confirmed."
)
final case class OrderConfirmedEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Customer who placed the order.")
    @encodedExample("7c9e6679-7425-40de-944b-e07fc1f90ae7")
    customerId: String,
    @description("When the order was confirmed (ISO-8601 instant).")
    @encodedExample("2026-10-31T10:15:30Z")
    timestamp: Instant
)

object OrderConfirmedEvent {
  implicit val codec: Codec[OrderConfirmedEvent] = deriveCodec
  implicit val schema: Schema[OrderConfirmedEvent] = Schema.derived
}

/** Emitted when order-service consumes the capture-failure `PaymentFailed` and
  * moves the order to `payment_failed`. `reason` carries payment-service's
  * capture-failure reason (e.g. `capture_failed`) for manual reconciliation.
  */
@description(
  "Payment capture failed: order-service consumed PaymentFailed and moved the order to payment_failed."
)
final case class OrderPaymentFailedEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Customer who placed the order.")
    @encodedExample("7c9e6679-7425-40de-944b-e07fc1f90ae7")
    customerId: String,
    @description("payment-service's capture-failure reason.")
    @encodedExample("capture_failed")
    reason: String,
    @description("When the order moved to payment_failed (ISO-8601 instant).")
    @encodedExample("2026-10-31T10:15:30Z")
    timestamp: Instant
)

object OrderPaymentFailedEvent {
  implicit val codec: Codec[OrderPaymentFailedEvent] = deriveCodec
  implicit val schema: Schema[OrderPaymentFailedEvent] = Schema.derived
}

/** Emitted when order-service moves an order to `cancelled`: a customer
  * `PATCH`/`PUT` (`reason = customer`) or a consumed
  * `PaymentAuthorizationExpired` (`reason = auth_expired`). `stale` and
  * `operator` are reserved for US-10/US-22.
  *
  * `reason` is optional in the wire schema so the registry's
  * `BACKWARD_TRANSITIVE` rule accepts the addition; this producer always sends
  * it, and consumers fall back for old reason-less messages.
  */
@description(
  "Order cancelled: a customer cancel or a consumed PaymentAuthorizationExpired moved the order to cancelled."
)
final case class OrderCancelledEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Customer who placed the order.")
    @encodedExample("7c9e6679-7425-40de-944b-e07fc1f90ae7")
    customerId: String,
    @description("When the order was cancelled (ISO-8601 instant).")
    @encodedExample("2026-10-31T10:15:30Z")
    timestamp: Instant,
    @description(
      "Cancellation trigger: customer | auth_expired | stale | operator. Always sent by the producer."
    )
    @encodedExample("customer")
    reason: Option[String] = None
)

object OrderCancelledEvent {
  implicit val codec: Codec[OrderCancelledEvent] = deriveCodec
  implicit val schema: Schema[OrderCancelledEvent] = Schema.derived
}
