package orderservice

sealed trait OrderError

case object OrderNotFound extends OrderError

final case class InvalidStatus(raw: String) extends OrderError
