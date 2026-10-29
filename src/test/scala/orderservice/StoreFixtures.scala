package orderservice

import cats.effect.IO
import orderservice.domain.{OrderId, OrderItemId}

/** Seeding helper shared by suites: builds a fully-populated order in any
  * status via the only write path (`createAuthorized`), replacing the removed
  * arbitrary `create`/`update` test hooks.
  */
object StoreFixtures {

  def seeded(
      store: OrderStore[IO],
      customerId: String,
      items: List[NewOrderItem],
      status: OrderStatus = OrderStatus.Authorized
  ): IO[(Order, List[OrderItem])] = {
    val orderId = OrderId.fromUuid(java.util.UUID.randomUUID())
    val orderItems = items.map(item =>
      OrderItem(
        OrderItemId.fromUuid(java.util.UUID.randomUUID()),
        orderId,
        item.sku,
        item.productName,
        item.unitPriceCents,
        item.quantity
      )
    )
    val total = items.map(i => i.unitPriceCents * i.quantity).sum
    val now = java.time.Instant.now()
    store.createAuthorized(
      Order(orderId, customerId, total, status, now, now),
      orderItems
    )
  }
}
