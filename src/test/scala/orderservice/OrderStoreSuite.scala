package orderservice

import cats.effect.IO
import io.circe.syntax._
import munit.CatsEffectSuite
import orderservice.domain.{
  OrderEvent,
  OrderId,
  OrderItemId,
  TransitionRejected
}

import java.time.Instant
import java.util.UUID

class OrderStoreSuite extends CatsEffectSuite with CreateAuthorizedContract {

  def withStore(name: String)(body: OrderStore[IO] => IO[Unit]): Unit =
    test(name)(OrderStore.inMemory[IO].flatMap(body))

  private def unknownOrderId: OrderId =
    OrderId.fromUuid(UUID.randomUUID())

  private val oneItem =
    List(NewOrderItem("sku-1", "Widget", 999, 2))

  private val twoItems =
    List(
      NewOrderItem("sku-1", "Widget", 999, 2),
      NewOrderItem("sku-2", "Gadget", 500, 3)
    )

  private def seeded(
      store: OrderStore[IO],
      status: OrderStatus,
      customerId: String = "cust-123",
      items: List[NewOrderItem] = twoItems,
      createdAt: Instant = Instant.parse("2026-01-01T00:00:00Z")
  ): IO[(Order, List[OrderItem])] = {
    val orderId = OrderId.fromUuid(UUID.randomUUID())
    val orderItems = items.map(item =>
      OrderItem(
        OrderItemId.fromUuid(UUID.randomUUID()),
        orderId,
        item.sku,
        item.productName,
        item.unitPriceCents,
        item.quantity
      )
    )
    val total = items.map(i => i.unitPriceCents * i.quantity).sum
    store.createAuthorized(
      Order(orderId, customerId, total, status, createdAt, createdAt),
      orderItems
    )
  }

  test("get returns the persisted entity and its items") {
    for {
      store <- OrderStore.inMemory[IO]
      created <- seeded(store, OrderStatus.Authorized)
      found <- store.get(created._1.id)
    } yield assertEquals(found, Some(created))
  }

  test("get returns None for an unknown id") {
    for {
      store <- OrderStore.inMemory[IO]
      found <- store.get(unknownOrderId)
    } yield assertEquals(found, None)
  }

  test(
    "delete removes the entity and its items, and get then returns None"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      (created, _) <- seeded(store, OrderStatus.Authorized, items = oneItem)
      deleted <- store.delete(created.id)
      found <- store.get(created.id)
    } yield {
      assert(deleted)
      assertEquals(found, None)
    }
  }

  test("delete returns false for an unknown id") {
    for {
      store <- OrderStore.inMemory[IO]
      deleted <- store.delete(unknownOrderId)
    } yield assert(!deleted)
  }

  test("transition applies an allowed event and returns the order's details") {
    for {
      store <- OrderStore.inMemory[IO]
      (created, _) <- seeded(store, OrderStatus.Authorized, items = oneItem)
      updated <- store.transition(created.id, OrderEvent.PaymentCaptured)
      found <- store.get(created.id)
    } yield {
      assertEquals(
        updated,
        Right(UpdatedOrderRef(created.id, "cust-123", created.totalCents))
      )
      assertEquals(found.map(_._1.status), Some(OrderStatus.Confirmed))
    }
  }

  test("transition rejects an illegal current status and changes nothing") {
    for {
      store <- OrderStore.inMemory[IO]
      (created, _) <- seeded(store, OrderStatus.Pending, items = oneItem)
      updated <- store.transition(created.id, OrderEvent.PaymentCaptured)
      found <- store.get(created.id)
    } yield {
      assertEquals(
        updated,
        Left(
          TransitionRejected.IllegalTransition(
            OrderStatus.Pending,
            OrderStatus.Confirmed
          )
        )
      )
      assertEquals(found.map(_._1.status), Some(OrderStatus.Pending))
    }
  }

  test("transition reports UnknownOrder for an unknown id") {
    for {
      store <- OrderStore.inMemory[IO]
      updated <- store.transition(unknownOrderId, OrderEvent.PaymentCaptured)
    } yield assertEquals(updated, Left(TransitionRejected.UnknownOrder))
  }

  test("a duplicate capture outcome is rejected, not reapplied") {
    for {
      (store, outbox) <- OrderStore.inMemoryWithOutbox[IO]
      (created, _) <- seeded(store, OrderStatus.Confirmed, items = oneItem)
      updated <- store.transition(created.id, OrderEvent.PaymentCaptured)
      events <- outbox.get
    } yield {
      assertEquals(
        updated,
        Left(
          TransitionRejected.IllegalTransition(
            OrderStatus.Confirmed,
            OrderStatus.Confirmed
          )
        )
      )
      assertEquals(events, Nil)
    }
  }

  test("a successful transition writes its outbox row") {
    for {
      (store, outbox) <- OrderStore.inMemoryWithOutbox[IO]
      (created, _) <- seeded(store, OrderStatus.Dispatched, items = oneItem)
      updated <- store.transition(
        created.id,
        OrderEvent.PaymentCaptured,
        Some(OrderConfirmedOutbox.event)
      )
      events <- outbox.get
    } yield {
      assert(updated.isRight)
      assertEquals(events.size, 1)
      val (aggregateId, envelope) = events.head
      assertEquals(aggregateId, OrderId.render(created.id))
      assertEquals(envelope.eventType, "OrderConfirmed")
      val payload = envelope.payload.as[OrderConfirmedEvent].toOption.get
      assertEquals(payload.orderId, OrderId.render(created.id))
      assertEquals(payload.customerId, "cust-123")
    }
  }

  test("a capture failure records its reconciliation reason") {
    for {
      (store, outbox) <- OrderStore.inMemoryWithOutbox[IO]
      (created, _) <- seeded(store, OrderStatus.Dispatched, items = oneItem)
      updated <- store.transition(
        created.id,
        OrderEvent.PaymentFailed("capture_failed"),
        Some(OrderPaymentFailedOutbox.event),
        reconciliationReason = Some("capture_failed")
      )
      found <- store.get(created.id)
      reason <- store.reconciliationReason(created.id)
      events <- outbox.get
    } yield {
      assert(updated.isRight)
      assertEquals(found.map(_._1.status), Some(OrderStatus.PaymentFailed))
      assertEquals(reason, Some("capture_failed"))
      assertEquals(events.size, 1)
      assertEquals(events.head._2.eventType, "OrderPaymentFailed")
    }
  }

  test("flagReconciliation records a flag without changing status") {
    for {
      store <- OrderStore.inMemory[IO]
      (created, _) <- seeded(store, OrderStatus.Authorized, items = oneItem)
      flagged <- store.flagReconciliation(created.id, "release failed")
      found <- store.get(created.id)
      reason <- store.reconciliationReason(created.id)
      missing <- store.flagReconciliation(unknownOrderId, "nope")
    } yield {
      assert(flagged)
      assertEquals(found.map(_._1.status), Some(OrderStatus.Authorized))
      assertEquals(reason, Some("release failed"))
      assert(!missing)
    }
  }

  test("reconciliationReason is None for a normal or unknown order") {
    for {
      store <- OrderStore.inMemory[IO]
      (order, _) <- seeded(store, OrderStatus.Authorized, items = oneItem)
      normal <- store.reconciliationReason(order.id)
      unknown <- store.reconciliationReason(unknownOrderId)
    } yield {
      assertEquals(normal, None)
      assertEquals(unknown, None)
    }
  }

  test("listByCustomer returns a customer's orders newest-first") {
    for {
      store <- OrderStore.inMemory[IO]
      (first, _) <- seeded(
        store,
        OrderStatus.Authorized,
        items = oneItem,
        createdAt = Instant.parse("2026-01-01T00:00:00Z")
      )
      (second, _) <- seeded(
        store,
        OrderStatus.Authorized,
        items = oneItem,
        createdAt = Instant.parse("2026-01-02T00:00:00Z")
      )
      history <- store.listByCustomer("cust-123")
    } yield assertEquals(history.map(_._1.id), List(second.id, first.id))
  }

  test("listByCustomer returns an empty list for a customer with no orders") {
    for {
      store <- OrderStore.inMemory[IO]
      history <- store.listByCustomer("unknown-customer")
    } yield assertEquals(history, Nil)
  }

  test("listByCustomer excludes orders belonging to a different customer") {
    for {
      store <- OrderStore.inMemory[IO]
      _ <- seeded(store, OrderStatus.Authorized, customerId = "cust-123")
      (other, _) <- seeded(
        store,
        OrderStatus.Authorized,
        customerId = "cust-456"
      )
      history <- store.listByCustomer("cust-456")
    } yield assertEquals(history.map(_._1.id), List(other.id))
  }
}

/** Small shared outbox-event factories so each suite asserts the same payload
  * shapes without copy-paste.
  */
object OrderConfirmedOutbox {
  private val at = Instant.parse("2026-01-01T00:00:00Z")
  val event: PendingOutboxEvent =
    PendingOutboxEvent(
      "OrderConfirmed",
      ref =>
        OrderConfirmedEvent(
          OrderId.render(ref.orderId),
          ref.customerId,
          at
        ).asJson
    )
}

object OrderPaymentFailedOutbox {
  private val at = Instant.parse("2026-01-01T00:00:00Z")
  val event: PendingOutboxEvent =
    PendingOutboxEvent(
      "OrderPaymentFailed",
      ref =>
        OrderPaymentFailedEvent(
          OrderId.render(ref.orderId),
          ref.customerId,
          "capture_failed",
          at
        ).asJson
    )
}
