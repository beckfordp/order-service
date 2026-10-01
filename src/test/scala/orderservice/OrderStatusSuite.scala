package orderservice

import munit.CatsEffectSuite

class OrderStatusSuite extends CatsEffectSuite {

  test("fromString parses \"pending\" as Pending") {
    assertEquals(OrderStatus.fromString("pending"), Right(OrderStatus.Pending))
  }

  test("fromString parses \"reserved\" as Reserved") {
    assertEquals(OrderStatus.fromString("reserved"), Right(OrderStatus.Reserved))
  }

  test("fromString parses \"reservation_failed\" as ReservationFailed") {
    assertEquals(
      OrderStatus.fromString("reservation_failed"),
      Right(OrderStatus.ReservationFailed)
    )
  }

  test("fromString rejects an unrecognized value") {
    assert(OrderStatus.fromString("bogus").isLeft)
  }

  test("asString round-trips through fromString for every status") {
    List(OrderStatus.Pending, OrderStatus.Reserved, OrderStatus.ReservationFailed)
      .foreach { status =>
        assertEquals(OrderStatus.fromString(status.asString), Right(status))
      }
  }
}
