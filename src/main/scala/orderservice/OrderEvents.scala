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
