package orderservice

import munit.FunSuite
import orderservice.domain.OrderId

class OrderIdSuite extends FunSuite {

  test("parse accepts a canonical UUID and render round-trips it") {
    val raw = "f47ac10b-58cc-4372-a567-0e02b2c3d479"
    assertEquals(OrderId.parse(raw).map(OrderId.render), Right(raw))
  }

  test("parse rejects a malformed id") {
    assert(OrderId.parse("not-a-uuid").isLeft)
  }

  test("parse rejects an empty string") {
    assert(OrderId.parse("").isLeft)
  }
}
