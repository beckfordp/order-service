package orderservice

import munit.CatsEffectSuite

class OrderStatusSuite extends CatsEffectSuite {

  test("fromString rejects the retired legacy states") {
    List("pending", "reserved", "reservation_failed").foreach { legacy =>
      assert(OrderStatus.fromString(legacy).isLeft, legacy)
    }
  }

  test("fromString parses \"confirmed\" as Confirmed") {
    assertEquals(
      OrderStatus.fromString("confirmed"),
      Right(OrderStatus.Confirmed)
    )
  }

  test("fromString parses \"payment_failed\" as PaymentFailed") {
    assertEquals(
      OrderStatus.fromString("payment_failed"),
      Right(OrderStatus.PaymentFailed)
    )
  }

  test("fromString parses \"cancelled\" as Cancelled") {
    assertEquals(
      OrderStatus.fromString("cancelled"),
      Right(OrderStatus.Cancelled)
    )
  }

  test("fromString parses \"authorized\" as Authorized") {
    assertEquals(
      OrderStatus.fromString("authorized"),
      Right(OrderStatus.Authorized)
    )
  }

  test("fromString rejects an unrecognized value") {
    assert(OrderStatus.fromString("bogus").isLeft)
  }

  test("asString round-trips through fromString for every status") {
    List(
      OrderStatus.Confirmed,
      OrderStatus.PaymentFailed,
      OrderStatus.Cancelled,
      OrderStatus.Authorized,
      OrderStatus.Dispatched,
      OrderStatus.DispatchFailed
    ).foreach { status =>
      assertEquals(OrderStatus.fromString(status.asString), Right(status))
    }
  }

  test("fromString parses dispatched as Dispatched") {
    assertEquals(
      OrderStatus.fromString("dispatched"),
      Right(OrderStatus.Dispatched)
    )
  }

  test("Dispatched serializes to dispatched") {
    assertEquals(OrderStatus.Dispatched.asString, "dispatched")
  }

  test("fromString parses dispatch_failed as DispatchFailed") {
    assertEquals(
      OrderStatus.fromString("dispatch_failed"),
      Right(OrderStatus.DispatchFailed)
    )
  }

  test("DispatchFailed serializes to dispatch_failed") {
    assertEquals(OrderStatus.DispatchFailed.asString, "dispatch_failed")
  }
}
