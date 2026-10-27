package orderservice

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.{Schema, Validator}

import java.time.Instant

/** Payload shape pinned in `gluon/docs/system-design.md`'s "Payload contracts"
  * section - that cross-repo doc, not this case class, is the source of truth
  * notification-service's consumer (US-7.1) should read. `status` is one of
  * `confirmed` (`PaymentCaptured`) / `payment_failed` (`PaymentFailed`) /
  * `cancelled` (`PaymentAuthorizationExpired`). Checkout itself publishes
  * nothing (ADR 0018): a failed checkout leaves no order, and `authorized` is
  * announced by payment-service's `PaymentAuthorized`. `reservation_failed`
  * stays in the enum only for events already on the topic: dropping it would
  * fail the registry's BACKWARD rule.
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
  // existing serialization - see PaymentEventConsumer), but only these 4 of
  // OrderStatus's values are ever published via
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
        OrderStatus.PaymentFailed.asString,
        OrderStatus.Cancelled.asString
      )
    )
    .encode(identity)

  implicit val schema: Schema[OrderStatusChangedEvent] =
    Schema
      .derived[OrderStatusChangedEvent]
      .modify(_.status)(_.validate(statusValidator))
}
