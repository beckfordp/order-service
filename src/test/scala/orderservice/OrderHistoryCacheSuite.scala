package orderservice

import cats.effect.IO
import com.dimafeng.testcontainers.RedisContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import munit.CatsEffectSuite
import org.typelevel.log4cats.noop.NoOpLogger

import scala.concurrent.duration._

class OrderHistoryCacheSuite extends CatsEffectSuite with TestContainerForAll {

  override val containerDef: RedisContainer.Def = RedisContainer.Def()

  private def oneOrder(customerId: String) = List(
    OrderResponse(
      id = "order-1",
      customerId = customerId,
      totalCents = 1998,
      status = "pending",
      items = Nil,
      createdAt = java.time.Instant.parse("2026-01-01T00:00:00Z"),
      updatedAt = java.time.Instant.parse("2026-01-01T00:00:00Z")
    )
  )

  // Unique customer id per test: TestContainerForAll shares one Redis
  // instance across every test in this suite, so a fixed key would leak
  // state between tests.
  private def newCustomerId(): String = java.util.UUID.randomUUID().toString

  test("get returns None for a customer with nothing cached") {
    withContainers { redis =>
      val config = RedisConfig(uri = redis.redisUri, historyTtlSeconds = 60)
      val customerId = newCustomerId()
      OrderHistoryCache.resource[IO](config, NoOpLogger[IO]).use { cache =>
        cache.get(customerId).map(assertEquals(_, None))
      }
    }
  }

  test("a cache miss followed by set populates the cache - subsequent get is a hit") {
    withContainers { redis =>
      val config = RedisConfig(uri = redis.redisUri, historyTtlSeconds = 60)
      val customerId = newCustomerId()
      OrderHistoryCache.resource[IO](config, NoOpLogger[IO]).use { cache =>
        for {
          before <- cache.get(customerId)
          _ <- cache.set(customerId, oneOrder(customerId))
          after <- cache.get(customerId)
        } yield {
          assertEquals(before, None)
          assertEquals(after, Some(oneOrder(customerId)))
        }
      }
    }
  }

  test("a cached entry expires after the configured TTL") {
    withContainers { redis =>
      val config = RedisConfig(uri = redis.redisUri, historyTtlSeconds = 1)
      val customerId = newCustomerId()
      OrderHistoryCache.resource[IO](config, NoOpLogger[IO]).use { cache =>
        for {
          _ <- cache.set(customerId, oneOrder(customerId))
          immediately <- cache.get(customerId)
          _ <- IO.sleep(2.seconds)
          afterTtl <- cache.get(customerId)
        } yield {
          assertEquals(immediately, Some(oneOrder(customerId)))
          assertEquals(afterTtl, None)
        }
      }
    }
  }
}
