// SPDX-License-Identifier: MIT

package orderservice

sealed trait OrderStatus {
  def asString: String = this match {
    case OrderStatus.Confirmed      => "confirmed"
    case OrderStatus.PaymentFailed  => "payment_failed"
    case OrderStatus.Cancelled      => "cancelled"
    case OrderStatus.Authorized     => "authorized"
    case OrderStatus.Dispatched     => "dispatched"
    case OrderStatus.DispatchFailed => "dispatch_failed"
  }
}

/** `Authorized` is where every order starts under ADR 0018 (no row until
  * payment is authorized). The pre-ADR-0018 `pending`/`reserved`/
  * `reservation_failed` states were retired by TD-21.
  */
object OrderStatus {
  case object Confirmed extends OrderStatus
  case object PaymentFailed extends OrderStatus
  case object Cancelled extends OrderStatus
  case object Authorized extends OrderStatus
  case object Dispatched extends OrderStatus
  case object DispatchFailed extends OrderStatus

  def fromString(raw: String): Either[String, OrderStatus] = raw match {
    case "confirmed"       => Right(Confirmed)
    case "payment_failed"  => Right(PaymentFailed)
    case "cancelled"       => Right(Cancelled)
    case "authorized"      => Right(Authorized)
    case "dispatched"      => Right(Dispatched)
    case "dispatch_failed" => Right(DispatchFailed)
    case other             => Left(s"Invalid order status: '$other'")
  }
}
