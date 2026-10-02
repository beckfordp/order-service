package orderservice

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec

import java.time.Instant

/** Payload shape pinned in `gluon/docs/system-design.md`'s "Payload contracts"
  * section - that cross-repo doc, not this case class, is the source of truth
  * payment-service's consumer (US-6.1) should read. Published exactly once per
  * order, at the moment it transitions from `pending` to `reserved` - see
  * `StockEventConsumer`.
  */
final case class OrderReservedEvent(
    orderId: String,
    customerId: String,
    totalCents: Int,
    timestamp: Instant
)

object OrderReservedEvent {
  implicit val codec: Codec[OrderReservedEvent] = deriveCodec
}

/** Payload shape pinned in `gluon/docs/system-design.md`'s "Payload contracts"
  * section - that cross-repo doc, not this case class, is the source of truth
  * notification-service's consumer (US-7.1) should read. `status` is one of
  * `reservation_failed` / `confirmed` / `payment_failed` - only
  * `reservation_failed` is published yet (US-5.4); the other two are US-6.3's
  * job, a separate track.
  */
final case class OrderStatusChangedEvent(
    orderId: String,
    customerId: String,
    status: String,
    timestamp: Instant
)

object OrderStatusChangedEvent {
  implicit val codec: Codec[OrderStatusChangedEvent] = deriveCodec
}
