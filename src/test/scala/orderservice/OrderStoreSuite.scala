package orderservice

import cats.effect.IO
import munit.CatsEffectSuite

class OrderStoreSuite extends CatsEffectSuite {

  test("create returns a persisted entity with a generated id") {
    for {
      store <- OrderStore.inMemory[IO]
      entity <- store.create("cust-123", 4999)
    } yield assert(entity.id.nonEmpty)
  }

  test("get returns the persisted entity") {
    for {
      store <- OrderStore.inMemory[IO]
      created <- store.create("cust-123", 4999)
      found <- store.get(created.id)
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
      first <- store.create("cust-123", 4999)
      second <- store.create("cust-123", 4999)
    } yield assertNotEquals(first.id, second.id)
  }

  test("update returns the updated entity with updatedAt not moving backwards") {
    for {
      store <- OrderStore.inMemory[IO]
      created <- store.create("cust-123", 4999)
      updated <- store.update(created.id, OrderStatus.Reserved)
    } yield {
      assertEquals(updated.map(_.id), Some(created.id))
      assert(
        updated.exists(!_.updatedAt.isBefore(created.updatedAt)),
        s"expected updatedAt not to move backwards, got: $updated"
      )
    }
  }

  test("update returns None for an unknown id") {
    for {
      store <- OrderStore.inMemory[IO]
      result <- store.update("unknown-id", OrderStatus.Pending)
    } yield assertEquals(result, None)
  }

  test("delete removes the entity and returns true, and get then returns None") {
    for {
      store <- OrderStore.inMemory[IO]
      created <- store.create("cust-123", 4999)
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
