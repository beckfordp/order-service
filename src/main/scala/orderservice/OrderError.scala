package orderservice

sealed trait OrderError

case object OrderNotFound extends OrderError

final case class InvalidStatus(raw: String) extends OrderError

case object EmptyOrderItems extends OrderError

final case class InvalidOrderItem(reason: String) extends OrderError
