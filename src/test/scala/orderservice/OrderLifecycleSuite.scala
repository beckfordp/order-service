package orderservice

import munit.ScalaCheckSuite
import orderservice.domain.{OrderEvent, OrderLifecycle, TransitionRejected}
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

class OrderLifecycleSuite extends ScalaCheckSuite {

  private val events = List(
    OrderEvent.PaymentCaptured,
    OrderEvent.PaymentFailed("capture_failed"),
    OrderEvent.AuthorizationExpired,
    OrderEvent.DispatchConfirmed,
    OrderEvent.DispatchFailed,
    OrderEvent.CustomerCancelled
  )

  test("payment outcomes accept authorized and dispatched orders") {
    for (from <- List(OrderStatus.Authorized, OrderStatus.Dispatched)) {
      assertEquals(
        OrderLifecycle.transition(from, OrderEvent.PaymentCaptured),
        Right(OrderStatus.Confirmed)
      )
      assertEquals(
        OrderLifecycle.transition(
          from,
          OrderEvent.PaymentFailed("capture_failed")
        ),
        Right(OrderStatus.PaymentFailed)
      )
    }
  }

  test("authorization expiry cancels only authorized orders") {
    assertEquals(
      OrderLifecycle.transition(
        OrderStatus.Authorized,
        OrderEvent.AuthorizationExpired
      ),
      Right(OrderStatus.Cancelled)
    )
    assert(
      OrderLifecycle
        .transition(OrderStatus.Dispatched, OrderEvent.AuthorizationExpired)
        .isLeft
    )
  }

  test("dispatch outcomes apply only to authorized orders") {
    assertEquals(
      OrderLifecycle.transition(
        OrderStatus.Authorized,
        OrderEvent.DispatchConfirmed
      ),
      Right(OrderStatus.Dispatched)
    )
    assertEquals(
      OrderLifecycle.transition(
        OrderStatus.Authorized,
        OrderEvent.DispatchFailed
      ),
      Right(OrderStatus.DispatchFailed)
    )
    assert(
      OrderLifecycle
        .transition(OrderStatus.Pending, OrderEvent.DispatchConfirmed)
        .isLeft
    )
  }

  test("customer cancellation is authorized -> cancelled only") {
    assertEquals(
      OrderLifecycle.transition(
        OrderStatus.Authorized,
        OrderEvent.CustomerCancelled
      ),
      Right(OrderStatus.Cancelled)
    )
    assert(
      OrderLifecycle
        .transition(OrderStatus.Dispatched, OrderEvent.CustomerCancelled)
        .isLeft
    )
  }

  test("illegal transitions name the current status and target") {
    assertEquals(
      OrderLifecycle.transition(
        OrderStatus.Confirmed,
        OrderEvent.PaymentCaptured
      ),
      Left(
        TransitionRejected.IllegalTransition(
          OrderStatus.Confirmed,
          OrderStatus.Confirmed
        )
      )
    )
  }

  property("terminal statuses accept no event") {
    val terminal = List(
      OrderStatus.Confirmed,
      OrderStatus.PaymentFailed,
      OrderStatus.Cancelled,
      OrderStatus.DispatchFailed
    )
    forAll(Gen.oneOf(terminal), Gen.oneOf(events)) { (status, event) =>
      assert(OrderLifecycle.transition(status, event).isLeft)
    }
  }

  private val allStatuses = List(
    OrderStatus.Pending,
    OrderStatus.Reserved,
    OrderStatus.ReservationFailed,
    OrderStatus.Authorized,
    OrderStatus.Dispatched,
    OrderStatus.Confirmed,
    OrderStatus.PaymentFailed,
    OrderStatus.Cancelled,
    OrderStatus.DispatchFailed
  )

  property("every event's allowed-from set matches transition") {
    forAll(Gen.oneOf(events), Gen.oneOf(allStatuses)) { (event, status) =>
      assertEquals(
        OrderLifecycle.transition(status, event).isRight,
        OrderLifecycle.allowedFrom(event).contains(status)
      )
    }
  }
}
