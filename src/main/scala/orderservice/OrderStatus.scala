package orderservice

sealed trait OrderStatus {
  def asString: String = this match {
    case OrderStatus.Pending           => "pending"
    case OrderStatus.Reserved          => "reserved"
    case OrderStatus.ReservationFailed => "reservation_failed"
  }
}

object OrderStatus {
  case object Pending extends OrderStatus
  case object Reserved extends OrderStatus
  case object ReservationFailed extends OrderStatus

  def fromString(raw: String): Either[String, OrderStatus] = raw match {
    case "pending"            => Right(Pending)
    case "reserved"           => Right(Reserved)
    case "reservation_failed" => Right(ReservationFailed)
    case other                => Left(s"Invalid order status: '$other'")
  }
}
