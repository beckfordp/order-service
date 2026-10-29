package orderservice

sealed trait OrderError

case object OrderNotFound extends OrderError

final case class InvalidStatus(raw: String) extends OrderError

/** A syntactically valid status that the lifecycle does not allow next (409).
  */
final case class IllegalTransition(to: String) extends OrderError

case object EmptyOrderItems extends OrderError

final case class InvalidOrderItem(reason: String) extends OrderError

/** A checkout request the client must fix (ADR 0018 `400`): bad shipping
  * address, malformed `paymentMethodId`, or an unknown payment method.
  */
final case class InvalidCheckout(message: String) extends OrderError

final case class PaymentDeclined(reason: String) extends OrderError

final case class InsufficientStock(failures: List[StockFailure])
    extends OrderError

case object PaymentServiceUnavailable extends OrderError

case object InventoryServiceUnavailable extends OrderError

case object OrderPersistFailed extends OrderError
