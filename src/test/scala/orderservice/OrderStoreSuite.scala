package orderservice

import cats.effect.IO
import munit.CatsEffectSuite

class OrderStoreSuite extends CatsEffectSuite {

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

  test("update returns the updated entity with updatedAt not moving backwards") {
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
}
