package orderservice

import cats.effect.IO
import io.circe.syntax._
import munit.CatsEffectSuite

import java.time.Instant
import scala.concurrent.duration._

class OrderStoreSuite extends CatsEffectSuite with CreateAuthorizedContract {

  def withStore(name: String)(body: OrderStore[IO] => IO[Unit]): Unit =
    test(name)(OrderStore.inMemory[IO].flatMap(body))

  private val oneItem =
    List(NewOrderItem("sku-1", "Widget", 999, 2))

  private val twoItems =
    List(
      NewOrderItem("sku-1", "Widget", 999, 2),
      NewOrderItem("sku-2", "Gadget", 500, 3)
    )

  test("create returns a persisted entity with a generated id") {
    for {
      store <- OrderStore.inMemory[IO]
      (order, _) <- store.create("cust-123", oneItem)
    } yield assert(order.id.nonEmpty)
  }

  test("create computes totalCents from the items") {
    for {
      store <- OrderStore.inMemory[IO]
      (order, _) <- store.create("cust-123", twoItems)
    } yield assertEquals(order.totalCents, 999 * 2 + 500 * 3)
  }

  test("create returns the persisted items, each with a generated id") {
    for {
      store <- OrderStore.inMemory[IO]
      (order, items) <- store.create("cust-123", twoItems)
    } yield {
      assertEquals(items.map(_.sku), List("sku-1", "sku-2"))
      assert(items.forall(_.id.nonEmpty))
      assert(items.forall(_.orderId == order.id))
    }
  }

  test("get returns the persisted entity and its items") {
    for {
      store <- OrderStore.inMemory[IO]
      created <- store.create("cust-123", twoItems)
      found <- store.get(created._1.id)
    } yield assertEquals(found, Some(created))
  }

  test("get returns None for an unknown id") {
    for {
      store <- OrderStore.inMemory[IO]
      found <- store.get("unknown-id")
    } yield assertEquals(found, None)
  }

  test("create produces distinct ids across calls") {
    for {
      store <- OrderStore.inMemory[IO]
      (first, _) <- store.create("cust-123", oneItem)
      (second, _) <- store.create("cust-123", oneItem)
    } yield assertNotEquals(first.id, second.id)
  }

  test(
    "update returns the updated entity with updatedAt not moving backwards"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      (created, _) <- store.create("cust-123", oneItem)
      updated <- store.update(created.id, OrderStatus.Reserved)
    } yield {
      assertEquals(updated.map(_._1.id), Some(created.id))
      assert(
        updated.exists(!_._1.updatedAt.isBefore(created.updatedAt)),
        s"expected updatedAt not to move backwards, got: $updated"
      )
    }
  }

  test("update preserves the order's items unchanged") {
    for {
      store <- OrderStore.inMemory[IO]
      (created, items) <- store.create("cust-123", twoItems)
      updated <- store.update(created.id, OrderStatus.Reserved)
    } yield assertEquals(updated.map(_._2), Some(items))
  }

  test("update returns None for an unknown id") {
    for {
      store <- OrderStore.inMemory[IO]
      result <- store.update("unknown-id", OrderStatus.Pending)
    } yield assertEquals(result, None)
  }

  test(
    "delete removes the entity and its items, and get then returns None"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      (created, _) <- store.create("cust-123", oneItem)
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
      deleted <- store.delete("unknown-id")
    } yield assert(!deleted)
  }

  test(
    "updateStatusIfCurrent transitions when the order's current status matches expected, returning the order's details"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      (created, _) <- store.create("cust-123", oneItem)
      _ <- store.update(created.id, OrderStatus.Reserved)
      updated <- store.updateStatusIfCurrent(
        created.id,
        OrderStatus.Reserved,
        OrderStatus.Confirmed
      )
      found <- store.get(created.id)
    } yield {
      assertEquals(
        updated,
        Some(UpdatedOrderRef(created.id, "cust-123", created.totalCents))
      )
      assertEquals(found.map(_._1.status), Some(OrderStatus.Confirmed))
    }
  }

  test(
    "updateStatusIfCurrent returns None and leaves status unchanged when the order's current status doesn't match expected"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      (created, _) <- store.create("cust-123", oneItem)
      updated <- store.updateStatusIfCurrent(
        created.id,
        OrderStatus.Reserved,
        OrderStatus.Confirmed
      )
      found <- store.get(created.id)
    } yield {
      assertEquals(updated, None)
      assertEquals(found.map(_._1.status), Some(OrderStatus.Pending))
    }
  }

  test("updateStatusIfCurrent returns None for an unknown order id") {
    for {
      store <- OrderStore.inMemory[IO]
      updated <- store.updateStatusIfCurrent(
        "unknown-order-id",
        OrderStatus.Reserved,
        OrderStatus.Confirmed
      )
    } yield assertEquals(updated, None)
  }

  test("listByCustomer returns a customer's orders newest-first") {
    for {
      store <- OrderStore.inMemory[IO]
      (first, _) <- store.create("cust-123", oneItem)
      _ <- IO.sleep(2.millis)
      (second, _) <- store.create("cust-123", oneItem)
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
      _ <- store.create("cust-123", oneItem)
      (other, _) <- store.create("cust-456", oneItem)
      history <- store.listByCustomer("cust-456")
    } yield assertEquals(history.map(_._1.id), List(other.id))
  }

  private val captureInstant = Instant.parse("2026-01-01T00:00:00Z")

  private def statusOutboxEvent(status: OrderStatus): PendingOutboxEvent =
    PendingOutboxEvent(
      "OrderStatusChanged",
      ref =>
        OrderStatusChangedEvent(
          ref.orderId,
          ref.customerId,
          status.asString,
          captureInstant
        ).asJson
    )

  private def seedOrder(
      store: OrderStore[IO],
      status: OrderStatus
  ): IO[Order] =
    store
      .create("cust-123", oneItem)
      .flatMap { case (order, _) =>
        store.update(order.id, status).map(_ => order)
      }

  test(
    "recordCaptureOutcome moves a dispatched order to confirmed and writes its outbox row"
  ) {
    for {
      (store, outbox) <- OrderStore.inMemoryWithOutbox[IO]
      order <- seedOrder(store, OrderStatus.Dispatched)
      updated <- store.recordCaptureOutcome(
        order.id,
        OrderStatus.Confirmed,
        statusOutboxEvent(OrderStatus.Confirmed)
      )
      found <- store.get(order.id)
      events <- outbox.get
    } yield {
      assertEquals(
        updated,
        Some(UpdatedOrderRef(order.id, "cust-123", order.totalCents))
      )
      assertEquals(found.map(_._1.status), Some(OrderStatus.Confirmed))
      assertEquals(events.size, 1)
      val (aggregateId, envelope) = events.head
      assertEquals(aggregateId, order.id)
      assertEquals(envelope.eventType, "OrderStatusChanged")
      val payload = envelope.payload.as[OrderStatusChangedEvent].toOption.get
      assertEquals(payload.status, "confirmed")
    }
  }

  test(
    "recordCaptureOutcome accepts an authorized order when the outcome overtook dispatch"
  ) {
    for {
      (store, outbox) <- OrderStore.inMemoryWithOutbox[IO]
      order <- seedOrder(store, OrderStatus.Authorized)
      updated <- store.recordCaptureOutcome(
        order.id,
        OrderStatus.Confirmed,
        statusOutboxEvent(OrderStatus.Confirmed)
      )
      found <- store.get(order.id)
      events <- outbox.get
    } yield {
      assert(updated.isDefined)
      assertEquals(found.map(_._1.status), Some(OrderStatus.Confirmed))
      assertEquals(events.size, 1)
    }
  }

  test("recordCaptureOutcome returns None for a non-waiting status") {
    for {
      (store, outbox) <- OrderStore.inMemoryWithOutbox[IO]
      order <- seedOrder(store, OrderStatus.Pending)
      updated <- store.recordCaptureOutcome(
        order.id,
        OrderStatus.Confirmed,
        statusOutboxEvent(OrderStatus.Confirmed)
      )
      found <- store.get(order.id)
      events <- outbox.get
    } yield {
      assertEquals(updated, None)
      assertEquals(found.map(_._1.status), Some(OrderStatus.Pending))
      assertEquals(events, Nil)
    }
  }

  test("a duplicate capture outcome is a no-op") {
    for {
      (store, outbox) <- OrderStore.inMemoryWithOutbox[IO]
      order <- seedOrder(store, OrderStatus.Confirmed)
      updated <- store.recordCaptureOutcome(
        order.id,
        OrderStatus.Confirmed,
        statusOutboxEvent(OrderStatus.Confirmed)
      )
      events <- outbox.get
    } yield {
      assertEquals(updated, None)
      assertEquals(events, Nil)
    }
  }

  test(
    "recordCaptureOutcome records a payment failure with its reconciliation reason"
  ) {
    for {
      (store, outbox) <- OrderStore.inMemoryWithOutbox[IO]
      order <- seedOrder(store, OrderStatus.Dispatched)
      updated <- store.recordCaptureOutcome(
        order.id,
        OrderStatus.PaymentFailed,
        statusOutboxEvent(OrderStatus.PaymentFailed),
        Some("capture_failed")
      )
      found <- store.get(order.id)
      reason <- store.reconciliationReason(order.id)
      events <- outbox.get
    } yield {
      assert(updated.isDefined)
      assertEquals(found.map(_._1.status), Some(OrderStatus.PaymentFailed))
      assertEquals(reason, Some("capture_failed"))
      assertEquals(events.size, 1)
      val payload = events.head._2.payload
        .as[OrderStatusChangedEvent]
        .toOption
        .get
      assertEquals(payload.status, "payment_failed")
    }
  }

  test("reconciliationReason is None for a normal or unknown order") {
    for {
      store <- OrderStore.inMemory[IO]
      (order, _) <- store.create("cust-123", oneItem)
      normal <- store.reconciliationReason(order.id)
      unknown <- store.reconciliationReason("unknown-order-id")
    } yield {
      assertEquals(normal, None)
      assertEquals(unknown, None)
    }
  }

  test("recordCaptureOutcome returns None for an unknown order id") {
    for {
      store <- OrderStore.inMemory[IO]
      updated <- store.recordCaptureOutcome(
        "unknown-order-id",
        OrderStatus.Confirmed,
        statusOutboxEvent(OrderStatus.Confirmed)
      )
      reason <- store.reconciliationReason("unknown-order-id")
    } yield {
      assertEquals(updated, None)
      assertEquals(reason, None)
    }
  }
}
