package orderservice.domain

import orderservice.OrderStatus

/** The lifecycle-relevant things that can happen to an order. HTTP, Kafka and
  * job paths all translate into one of these before touching status.
  */
sealed trait OrderEvent {
  def label: String
}

object OrderEvent {
  case object PaymentCaptured extends OrderEvent {
    val label = "PaymentCaptured"
  }
  final case class PaymentFailed(reason: String) extends OrderEvent {
    val label = "PaymentFailed"
  }
  case object AuthorizationExpired extends OrderEvent {
    val label = "AuthorizationExpired"
  }
  case object DispatchConfirmed extends OrderEvent {
    val label = "DispatchConfirmed"
  }
  case object DispatchFailed extends OrderEvent { val label = "DispatchFailed" }
  case object CustomerCancelled extends OrderEvent {
    val label = "CustomerCancelled"
  }
}

sealed trait TransitionRejected

object TransitionRejected {
  case object UnknownOrder extends TransitionRejected

  final case class IllegalTransition(
      from: OrderStatus,
      to: OrderStatus
  ) extends TransitionRejected
}

/** The single source of truth for order status changes (scala-fp.md law 2).
  * `allowedFrom` is also what the store passes to its atomic conditional
  * UPDATE, so the rule is defined once and enforced twice.
  */
object OrderLifecycle {

  private val awaitingCapture: Set[OrderStatus] =
    Set(OrderStatus.Authorized, OrderStatus.Dispatched)

  def allowedFrom(event: OrderEvent): Set[OrderStatus] =
    event match {
      case OrderEvent.PaymentCaptured      => awaitingCapture
      case OrderEvent.PaymentFailed(_)     => awaitingCapture
      case OrderEvent.AuthorizationExpired =>
        Set[OrderStatus](OrderStatus.Authorized)
      case OrderEvent.DispatchConfirmed =>
        Set[OrderStatus](OrderStatus.Authorized)
      case OrderEvent.DispatchFailed => Set[OrderStatus](OrderStatus.Authorized)
      case OrderEvent.CustomerCancelled =>
        Set[OrderStatus](OrderStatus.Authorized)
    }

  def target(event: OrderEvent): OrderStatus =
    event match {
      case OrderEvent.PaymentCaptured      => OrderStatus.Confirmed
      case OrderEvent.PaymentFailed(_)     => OrderStatus.PaymentFailed
      case OrderEvent.AuthorizationExpired => OrderStatus.Cancelled
      case OrderEvent.DispatchConfirmed    => OrderStatus.Dispatched
      case OrderEvent.DispatchFailed       => OrderStatus.DispatchFailed
      case OrderEvent.CustomerCancelled    => OrderStatus.Cancelled
    }

  def transition(
      current: OrderStatus,
      event: OrderEvent
  ): Either[TransitionRejected.IllegalTransition, OrderStatus] =
    if (allowedFrom(event).contains(current)) Right(target(event))
    else
      Left(
        TransitionRejected.IllegalTransition(current, target(event))
      )
}
