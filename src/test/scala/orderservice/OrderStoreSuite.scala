package orderservice

import cats.effect.IO
import io.circe.syntax._
import munit.CatsEffectSuite
import orderservice.domain.{
  OrderEvent,
  OrderId,
  OrderItemId,
  ReconciliationStatus,
  ResolveReconciliationOutcome,
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
      (created, _) <- seeded(store, OrderStatus.Cancelled, items = oneItem)
      updated <- store.transition(created.id, OrderEvent.PaymentCaptured)
      found <- store.get(created.id)
    } yield {
      assertEquals(
        updated,
        Left(
          TransitionRejected.IllegalTransition(
            OrderStatus.Cancelled,
            OrderStatus.Confirmed
          )
        )
      )
      assertEquals(found.map(_._1.status), Some(OrderStatus.Cancelled))
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

  test("cancellation transitions write the audit actor and reason") {
    for {
      store <- OrderStore.inMemory[IO]
      (customerOrder, _) <- seeded(
        store,
        OrderStatus.Authorized,
        items = oneItem
      )
      _ <- store.transition(
        customerOrder.id,
        OrderEvent.CustomerCancelled
      )
      customerAudit <- store.cancellationAudit(customerOrder.id)
      (operatorOrder, _) <- seeded(
        store,
        OrderStatus.Authorized,
        items = oneItem
      )
      _ <- store.transition(
        operatorOrder.id,
        OrderEvent.OperatorCancelled("op-7")
      )
      operatorAudit <- store.cancellationAudit(operatorOrder.id)
      (expiredOrder, _) <- seeded(
        store,
        OrderStatus.Authorized,
        items = oneItem
      )
      _ <- store.transition(
        expiredOrder.id,
        OrderEvent.AuthorizationExpired
      )
      systemAudit <- store.cancellationAudit(expiredOrder.id)
    } yield {
      assertEquals(
        customerAudit.map(a => (a.actorType, a.actorId, a.reason)),
        Some(("customer", customerOrder.customerId, "customer"))
      )
      assertEquals(
        operatorAudit.map(a => (a.actorType, a.actorId, a.reason)),
        Some(("operator", "op-7", "operator"))
      )
      assertEquals(
        systemAudit.map(a => (a.actorType, a.actorId, a.reason)),
        Some(("system", "payment-service", "auth_expired"))
      )
    }
  }

  test(
    "cancel racing authorization expiry produces one transition and one event"
  ) {
    def cancellationOutbox(reason: String): PendingOutboxEvent =
      PendingOutboxEvent(
        "OrderCancelled",
        ref =>
          OrderCancelledEvent(
            OrderId.render(ref.orderId),
            ref.customerId,
            Instant.EPOCH,
            reason = Some(reason),
            actor =
              if (reason == "customer")
                Some(OrderActor.customer(ref.customerId))
              else Some(OrderActor.system("payment-service"))
          ).asJson
      )

    for {
      storeAndOutbox <- OrderStore.inMemoryWithOutbox[IO]
      (store, outbox) = storeAndOutbox
      (order, _) <- seeded(store, OrderStatus.Authorized, items = oneItem)
      results <- IO.both(
        store.transition(
          order.id,
          OrderEvent.CustomerCancelled,
          Some(cancellationOutbox("customer"))
        ),
        store.transition(
          order.id,
          OrderEvent.AuthorizationExpired,
          Some(cancellationOutbox("auth_expired"))
        )
      )
      events <- outbox.get
      audit <- store.cancellationAudit(order.id)
    } yield {
      val winners =
        List(results._1.isRight, results._2.isRight).count(identity)
      assertEquals(winners, 1)
      assertEquals(events.size, 1)
      assert(audit.isDefined)
    }
  }

  test("reconciliation flags are first-wins") {
    for {
      store <- OrderStore.inMemory[IO]
      (created, _) <- seeded(store, OrderStatus.Authorized, items = oneItem)
      _ <- store.flagReconciliation(created.id, "first")
      _ <- store.flagReconciliation(created.id, "second")
      flagged <- store.reconciliationReason(created.id)
      (transitioned, _) <- seeded(
        store,
        OrderStatus.Authorized,
        items = oneItem
      )
      _ <- store.transition(
        transitioned.id,
        OrderEvent.PaymentCaptured,
        reconciliationReason = Some("transition-first")
      )
      _ <- store.flagReconciliation(transitioned.id, "later")
      transitionReason <- store.reconciliationReason(transitioned.id)
    } yield {
      assertEquals(flagged, Some("first"))
      assertEquals(transitionReason, Some("transition-first"))
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

  test("listAll returns every order newest-first, with optional filters") {
    for {
      store <- OrderStore.inMemory[IO]
      (older, _) <- seeded(
        store,
        OrderStatus.Authorized,
        customerId = "cust-123",
        createdAt = Instant.parse("2026-01-01T00:00:00Z")
      )
      (newer, _) <- seeded(
        store,
        OrderStatus.Cancelled,
        customerId = "cust-456",
        createdAt = Instant.parse("2026-01-02T00:00:00Z")
      )
      all <- store.listAll(None, None)
      authorized <- store.listAll(Some(OrderStatus.Authorized), None)
      byCustomer <- store.listAll(None, Some("cust-456"))
      none <- store.listAll(Some(OrderStatus.Confirmed), None)
    } yield {
      assertEquals(all.map(_._1.id), List(newer.id, older.id))
      assertEquals(authorized.map(_._1.id), List(older.id))
      assertEquals(byCustomer.map(_._1.id), List(newer.id))
      assertEquals(none, Nil)
    }
  }

  test("listReconciliations filters by status and orderId") {
    for {
      store <- OrderStore.inMemory[IO]
      (first, _) <- seeded(store, OrderStatus.PaymentFailed)
      (second, _) <- seeded(store, OrderStatus.PaymentFailed)
      _ <- store.flagReconciliation(first.id, "capture_after_void")
      _ <- store.flagReconciliation(second.id, "dispatch_after_release")
      _ <- store.resolveReconciliation(
        first.id,
        "op-1",
        None,
        Instant.parse("2026-11-02T10:00:00Z")
      )
      open <- store.listReconciliations(Some(ReconciliationStatus.Open), None)
      resolved <- store.listReconciliations(
        Some(ReconciliationStatus.Resolved),
        None
      )
      byOrder <- store.listReconciliations(None, Some(second.id))
    } yield {
      assertEquals(open.map(_.orderId), List(second.id))
      assertEquals(resolved.map(_.orderId), List(first.id))
      assertEquals(byOrder.map(_.reason), List("dispatch_after_release"))
    }
  }

  test("resolveReconciliation is idempotent and never overwrites") {
    val at = Instant.parse("2026-11-02T10:00:00Z")
    for {
      store <- OrderStore.inMemory[IO]
      (order, _) <- seeded(store, OrderStatus.PaymentFailed)
      _ <- store.flagReconciliation(order.id, "capture_after_void")
      first <- store.resolveReconciliation(
        order.id,
        "op-1",
        Some("refunded"),
        at
      )
      second <- store.resolveReconciliation(
        order.id,
        "op-2",
        Some("later"),
        at.plusSeconds(60)
      )
      stored <- store.listReconciliations(None, Some(order.id))
    } yield {
      first match {
        case ResolveReconciliationOutcome.Resolved(row) =>
          assertEquals(row.status, ReconciliationStatus.Resolved)
          assertEquals(row.operatorId, Some("op-1"))
          assertEquals(row.note, Some("refunded"))
          assertEquals(row.resolvedAt, Some(at))
        case other => fail(s"expected Resolved, got: $other")
      }
      assert(second.isInstanceOf[ResolveReconciliationOutcome.AlreadyResolved])
      assertEquals(stored.map(_.operatorId), List(Some("op-1")))
      assertEquals(stored.map(_.note), List(Some("refunded")))
    }
  }

  test("resolveReconciliation reports NotFound for an un-flagged order") {
    for {
      store <- OrderStore.inMemory[IO]
      (order, _) <- seeded(store, OrderStatus.Authorized)
      result <- store.resolveReconciliation(
        order.id,
        "op-1",
        None,
        Instant.parse("2026-11-02T10:00:00Z")
      )
    } yield assertEquals(result, ResolveReconciliationOutcome.NotFound)
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
