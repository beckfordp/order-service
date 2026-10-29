package orderservice

import cats.effect.IO
import munit.CatsEffectSuite
import orderservice.domain.{OrderEvent, OrderId, OrderItemId}

import java.time.Instant
import java.util.UUID

/** `OrderStore.createAuthorized` behaviour (ADR 0018, US-11.1) shared by the
  * in-memory and Postgres suites so the two interpreters can't drift.
  */
trait CreateAuthorizedContract { self: CatsEffectSuite =>

  def withStore(name: String)(body: OrderStore[IO] => IO[Unit]): Unit

  private val address =
    Address("1 High St", Some("Flat 2"), "London", "SW1A 1AA", "GB")

  private def authorizedOrder(customerId: String) = {
    val orderId = OrderId.fromUuid(UUID.randomUUID())
    val now = Instant.now()
    val items = List(
      OrderItem(
        OrderItemId.fromUuid(UUID.randomUUID()),
        orderId,
        "sku-1",
        "Widget",
        999,
        2
      ),
      OrderItem(
        OrderItemId.fromUuid(UUID.randomUUID()),
        orderId,
        "sku-2",
        "Gadget",
        500,
        1
      )
    )
    (
      Order(
        orderId,
        customerId,
        2498,
        OrderStatus.Authorized,
        now,
        now,
        Some(address)
      ),
      items
    )
  }

  withStore(
    "createAuthorized persists the pre-minted order and items as authorized"
  ) { store =>
    val (order, items) = authorizedOrder(s"cust-${UUID.randomUUID()}")
    for {
      created <- store.createAuthorized(order, items)
      found <- store.get(order.id)
    } yield {
      assertEquals(created._1.id, order.id)
      assertEquals(created._1.status, OrderStatus.Authorized)
      assertEquals(created._1.shippingAddress, Some(address))
      assertEquals(created._2.map(_.id).toSet, items.map(_.id).toSet)
      assertEquals(found.map(_._1.status), Some(OrderStatus.Authorized))
      assertEquals(found.map(_._1.shippingAddress), Some(Some(address)))
      assertEquals(found.map(_._2.map(_.id).toSet), Some(items.map(_.id).toSet))
    }
  }

  withStore("listByCustomer and transition return the shipping address") {
    store =>
      val customerId = s"cust-${UUID.randomUUID()}"
      val (order, items) = authorizedOrder(customerId)
      for {
        _ <- store.createAuthorized(order, items)
        listed <- store.listByCustomer(customerId)
        transitioned <- store.transition(
          order.id,
          OrderEvent.CustomerCancelled
        )
        found <- store.get(order.id)
      } yield {
        assertEquals(listed.map(_._1.shippingAddress), List(Some(address)))
        assert(transitioned.isRight)
        assertEquals(found.map(_._1.shippingAddress), Some(Some(address)))
        assertEquals(found.map(_._1.status), Some(OrderStatus.Cancelled))
      }
  }

  withStore("orders created without an address read back as None") { store =>
    val now = Instant.now()
    val orderId = OrderId.fromUuid(UUID.randomUUID())
    val order = Order(
      orderId,
      s"cust-${UUID.randomUUID()}",
      999,
      OrderStatus.Authorized,
      now,
      now,
      None
    )
    val items = List(
      OrderItem(
        OrderItemId.fromUuid(UUID.randomUUID()),
        orderId,
        "sku-1",
        "Widget",
        999,
        1
      )
    )
    for {
      _ <- store.createAuthorized(order, items)
      found <- store.get(orderId)
    } yield assertEquals(found.map(_._1.shippingAddress), Some(None))
  }
}
