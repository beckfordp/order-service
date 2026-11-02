package orderservice.domain

import java.time.Instant

/** Open/resolved state of a manual-reconciliation flag (ADM-4, ADR 0026). */
sealed trait ReconciliationStatus { def asString: String }

object ReconciliationStatus {
  case object Open extends ReconciliationStatus {
    val asString: String = "open"
  }
  case object Resolved extends ReconciliationStatus {
    val asString: String = "resolved"
  }

  val all: List[ReconciliationStatus] = List(Open, Resolved)

  def fromString(raw: String): Either[String, ReconciliationStatus] =
    all
      .find(_.asString == raw)
      .toRight(
        s"Invalid reconciliation status: '$raw'"
      )
}

/** One first-wins reconciliation flag, plus its operator resolution (ADM-4). A
  * resolved flag is terminal: the recording path never reopens it.
  */
final case class Reconciliation(
    orderId: OrderId,
    reason: String,
    status: ReconciliationStatus,
    createdAt: Instant,
    operatorId: Option[String] = None,
    note: Option[String] = None,
    resolvedAt: Option[Instant] = None
) {

  /** Pure resolve rule. `None` means it is already resolved, so the caller
    * returns the existing row untouched (idempotent resolve).
    */
  def resolve(
      operatorId: String,
      note: Option[String],
      at: Instant
  ): Option[Reconciliation] =
    status match {
      case ReconciliationStatus.Resolved => None
      case ReconciliationStatus.Open     =>
        Some(
          copy(
            status = ReconciliationStatus.Resolved,
            operatorId = Some(operatorId),
            note = note,
            resolvedAt = Some(at)
          )
        )
    }
}

sealed trait ResolveReconciliationOutcome

object ResolveReconciliationOutcome {
  final case class Resolved(row: Reconciliation)
      extends ResolveReconciliationOutcome
  final case class AlreadyResolved(row: Reconciliation)
      extends ResolveReconciliationOutcome
  case object NotFound extends ResolveReconciliationOutcome
}
