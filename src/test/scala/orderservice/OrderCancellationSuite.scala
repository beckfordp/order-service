// SPDX-License-Identifier: MIT

package orderservice

import munit.FunSuite
import orderservice.domain.{
  CancellationAudit,
  OrderCancellation,
  OrderEvent,
  OrderLifecycle
}

import java.time.Instant
import java.util.UUID

class OrderCancellationSuite extends FunSuite {

  private val ref = UpdatedOrderRef(
    orderservice.domain.OrderId.fromUuid(UUID.randomUUID()),
    "cust-1",
    4999,
    OrderStatus.Authorized,
    java.time.Instant.EPOCH
  )
  private val now = Instant.parse("2026-11-02T10:00:00Z")

  test("customer cancellation maps to the customer actor") {
    assertEquals(
      OrderCancellation.actorFor(OrderEvent.CustomerCancelled, ref),
      Some(OrderActor.customer("cust-1"))
    )
    assertEquals(
      OrderCancellation.reasonFor(OrderEvent.CustomerCancelled),
      Some("customer")
    )
    assertEquals(
      OrderCancellation.auditFor(OrderEvent.CustomerCancelled, ref, now),
      Some(
        CancellationAudit(
          "customer",
          "cust-1",
          "customer",
          now
        )
      )
    )
  }

  test("operator cancellation maps to the operator actor") {
    val event = OrderEvent.OperatorCancelled("op-7")
    assertEquals(
      OrderCancellation.actorFor(event, ref),
      Some(OrderActor.operator("op-7"))
    )
    assertEquals(OrderCancellation.reasonFor(event), Some("operator"))
  }

  test("authorization expiry maps to the payment service system actor") {
    assertEquals(
      OrderCancellation.actorFor(OrderEvent.AuthorizationExpired, ref),
      Some(OrderActor.system("payment-service"))
    )
    assertEquals(
      OrderCancellation.reasonFor(OrderEvent.AuthorizationExpired),
      Some("auth_expired")
    )
  }

  test("non-cancellation events have no actor, reason or audit") {
    List(
      OrderEvent.PaymentCaptured,
      OrderEvent.PaymentFailed("x"),
      OrderEvent.DispatchConfirmed,
      OrderEvent.DispatchFailed
    ).foreach { event =>
      assertEquals(OrderCancellation.actorFor(event, ref), None)
      assertEquals(OrderCancellation.reasonFor(event), None)
      assertEquals(OrderCancellation.auditFor(event, ref, now), None)
    }
  }

  test("every event has both a reason and an actor, or neither") {
    List(
      OrderEvent.PaymentCaptured,
      OrderEvent.PaymentFailed("x"),
      OrderEvent.AuthorizationExpired,
      OrderEvent.DispatchConfirmed,
      OrderEvent.DispatchFailed,
      OrderEvent.CustomerCancelled,
      OrderEvent.OperatorCancelled("op")
    ).foreach { event =>
      assertEquals(
        OrderCancellation.reasonFor(event).isDefined,
        OrderCancellation.actorFor(event, ref).isDefined,
        event
      )
    }
  }

  test(
    "operator cancellation is only legal from Authorized and targets Cancelled"
  ) {
    assertEquals(
      OrderLifecycle
        .transition(OrderStatus.Authorized, OrderEvent.OperatorCancelled("op")),
      Right(OrderStatus.Cancelled)
    )
    assertEquals(
      OrderLifecycle.target(OrderEvent.OperatorCancelled("op")),
      OrderStatus.Cancelled
    )
    assert(
      OrderLifecycle
        .transition(OrderStatus.Dispatched, OrderEvent.OperatorCancelled("op"))
        .isLeft
    )
    assert(
      OrderLifecycle
        .transition(OrderStatus.Cancelled, OrderEvent.OperatorCancelled("op"))
        .isLeft
    )
  }
}
