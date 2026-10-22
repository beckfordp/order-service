package orderservice

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.{Schema, Validator}

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
  implicit val schema: Schema[OrderReservedEvent] = Schema.derived
}

/** Payload shape pinned in `gluon/docs/system-design.md`'s "Payload contracts"
  * section - that cross-repo doc, not this case class, is the source of truth
  * notification-service's consumer (US-7.1) should read. `status` is one of
  * `reservation_failed` (`StockEventConsumer`, US-5.4) / `confirmed` /
  * `payment_failed` (`PaymentEventConsumer`, US-6.3) - never `pending` or
  * `reserved`, `OrderStatus`'s other two values, which this event is never
  * published for.
  */
final case class OrderStatusChangedEvent(
    orderId: String,
    customerId: String,
    status: String,
    timestamp: Instant
)

object OrderStatusChangedEvent {
  implicit val codec: Codec[OrderStatusChangedEvent] = deriveCodec

  // status is a plain String on the wire (matching OrderStatus.asString, the
  // existing serialization - see StockEventConsumer/PaymentEventConsumer),
  // but only 3 of OrderStatus's 5 values are ever actually published via
  // this event (see this class's own doc comment) - Validator.enumeration
  // documents that fixed set in the generated AsyncAPI JSON Schema
  // ("enum": [...]) without changing the wire format. The `.encode(identity)`
  // step is required: tapir-apispec-docs silently drops an enumeration
  // validator with no encode function rather than rendering it.
  private val statusValidator: Validator[String] = Validator
    .enumeration(
      List(
        OrderStatus.ReservationFailed.asString,
        OrderStatus.Confirmed.asString,
        OrderStatus.PaymentFailed.asString
      )
    )
    .encode(identity)

  implicit val schema: Schema[OrderStatusChangedEvent] =
    Schema
      .derived[OrderStatusChangedEvent]
      .modify(_.status)(_.validate(statusValidator))
}
