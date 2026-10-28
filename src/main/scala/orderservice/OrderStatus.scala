package orderservice

sealed trait OrderStatus {
  def asString: String = this match {
    case OrderStatus.Pending           => "pending"
    case OrderStatus.Reserved          => "reserved"
    case OrderStatus.ReservationFailed => "reservation_failed"
    case OrderStatus.Confirmed         => "confirmed"
    case OrderStatus.PaymentFailed     => "payment_failed"
    case OrderStatus.Cancelled         => "cancelled"
    case OrderStatus.Authorized        => "authorized"
    case OrderStatus.Dispatched        => "dispatched"
  }
}

/** `Authorized` is where every order starts under ADR 0018 (no row until
  * payment is authorized). `Pending`/`Reserved`/`ReservationFailed` are no
  * longer produced; they stay so orders written before ADR 0018 still load.
  */
object OrderStatus {
  case object Pending extends OrderStatus
  case object Reserved extends OrderStatus
  case object ReservationFailed extends OrderStatus
  case object Confirmed extends OrderStatus
  case object PaymentFailed extends OrderStatus
  case object Cancelled extends OrderStatus
  case object Authorized extends OrderStatus
  case object Dispatched extends OrderStatus

  def fromString(raw: String): Either[String, OrderStatus] = raw match {
    case "pending"            => Right(Pending)
    case "reserved"           => Right(Reserved)
    case "reservation_failed" => Right(ReservationFailed)
    case "confirmed"          => Right(Confirmed)
    case "payment_failed"     => Right(PaymentFailed)
    case "cancelled"          => Right(Cancelled)
    case "authorized"         => Right(Authorized)
    case "dispatched"         => Right(Dispatched)
    case other                => Left(s"Invalid order status: '$other'")
  }
}
