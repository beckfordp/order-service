package orderservice

import cats.effect.IO
import munit.CatsEffectSuite
import orderservice.domain.{OrderId, Reconciliation, ReconciliationStatus}

import java.time.Instant
import java.util.UUID

class ReconciliationDomainSuite extends CatsEffectSuite {

  private val orderId = OrderId.fromUuid(UUID.randomUUID())
  private val createdAt = Instant.parse("2026-11-02T10:00:00Z")
  private val resolvedAt = Instant.parse("2026-11-02T11:00:00Z")

  private def open: Reconciliation =
    Reconciliation(
      orderId,
      "capture_after_void",
      ReconciliationStatus.Open,
      createdAt
    )

  test("status parsing accepts only open and resolved") {
    assertEquals(
      ReconciliationStatus.fromString("open"),
      Right(ReconciliationStatus.Open)
    )
    assertEquals(
      ReconciliationStatus.fromString("resolved"),
      Right(ReconciliationStatus.Resolved)
    )
    assert(ReconciliationStatus.fromString("bogus").isLeft)
  }

  test("resolving an open flag records the operator, note and timestamp") {
    val resolved = open.resolve("op-1", Some("refunded"), resolvedAt)
    assertEquals(
      resolved.map(row =>
        (row.status, row.operatorId, row.note, row.resolvedAt)
      ),
      Some(
        (
          ReconciliationStatus.Resolved,
          Some("op-1"),
          Some("refunded"),
          Some(resolvedAt)
        )
      )
    )
  }

  test("a resolved flag is terminal") {
    val resolved = open
      .resolve("op-1", Some("refunded"), resolvedAt)
      .getOrElse(fail("open flag must resolve"))
    assertEquals(resolved.resolve("op-2", None, resolvedAt), None)
  }
}
