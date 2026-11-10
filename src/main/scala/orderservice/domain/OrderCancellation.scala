// SPDX-License-Identifier: MIT

package orderservice.domain

import orderservice.{OrderActor, UpdatedOrderRef}

import java.time.Instant

/** Durable row written with the cancellation transition (US-22). */
final case class CancellationAudit(
    actorType: String,
    actorId: String,
    reason: String,
    createdAt: Instant
)

/** The single pure mapping from a cancellation event to its reason, wire actor
  * and audit row, so the HTTP and Kafka paths cannot disagree.
  */
object OrderCancellation {

  def reasonFor(event: OrderEvent): Option[String] =
    event match {
      case OrderEvent.CustomerCancelled    => Some("customer")
      case OrderEvent.OperatorCancelled(_) => Some("operator")
      case OrderEvent.AuthorizationExpired => Some("auth_expired")
      case OrderEvent.PaymentCaptured | OrderEvent.PaymentFailed(_) |
          OrderEvent.DispatchConfirmed | OrderEvent.DispatchFailed =>
        None
    }

  def actorFor(
      event: OrderEvent,
      ref: UpdatedOrderRef
  ): Option[OrderActor] =
    event match {
      case OrderEvent.CustomerCancelled =>
        Some(OrderActor.customer(ref.customerId))
      case OrderEvent.OperatorCancelled(operatorId) =>
        Some(OrderActor.operator(operatorId))
      case OrderEvent.AuthorizationExpired =>
        Some(OrderActor.system("payment-service"))
      case OrderEvent.PaymentCaptured | OrderEvent.PaymentFailed(_) |
          OrderEvent.DispatchConfirmed | OrderEvent.DispatchFailed =>
        None
    }

  def auditFor(
      event: OrderEvent,
      ref: UpdatedOrderRef,
      now: Instant
  ): Option[CancellationAudit] =
    for {
      actor <- actorFor(event, ref)
      reason <- reasonFor(event)
    } yield CancellationAudit(actor.`type`, actor.id, reason, now)
}
