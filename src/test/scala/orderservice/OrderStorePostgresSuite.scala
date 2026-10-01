package orderservice

import cats.effect.IO
import cats.syntax.all._
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import munit.CatsEffectSuite
import org.testcontainers.utility.DockerImageName
import org.typelevel.otel4s.metrics.Meter
import purerest.metrics.Metrics

import scala.jdk.CollectionConverters._

class OrderStorePostgresSuite
    extends CatsEffectSuite
    with TestContainerForAll {

  override val containerDef: PostgreSQLContainer.Def =
    PostgreSQLContainer.Def(dockerImageName =
      DockerImageName.parse("postgres:16-alpine")
    )

  private def configFor(postgres: PostgreSQLContainer): PostgresConfig =
    PostgresConfig(
      host = postgres.host,
      port = postgres.mappedPort(5432),
      database = postgres.databaseName,
      user = postgres.username,
      password = postgres.password
    )

  test("create persists an entity and returns it with a generated id") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          store.create("cust-123", 4999).map { entity =>
            assert(entity.id.nonEmpty)
          }
        }
    }
  }

  test("create produces distinct ids across calls") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations
        .run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            first <- store.create("cust-123", 4999)
            second <- store.create("cust-123", 4999)
          } yield assertNotEquals(first.id, second.id)
        }
    }
  }

  test("get returns the persisted entity") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            created <- store.create("cust-123", 4999)
            found <- store.get(created.id)
          } yield assertEquals(found, Some(created))
        }
    }
  }

  test("get returns None for an unknown id") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          store
            .get(java.util.UUID.randomUUID().toString)
            .map(assertEquals(_, None))
        }
    }
  }

  test("get returns None for a malformed (non-UUID) id") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          store.get("not-a-uuid").map(assertEquals(_, None))
        }
    }
  }

  test("update returns the updated entity and returns it") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
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
    }
  }

  test("update returns None for an unknown id") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          store
            .update(java.util.UUID.randomUUID().toString, OrderStatus.Pending)
            .map(assertEquals(_, None))
        }
    }
  }

  test("update returns None for a malformed (non-UUID) id") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          store
            .update("not-a-uuid", OrderStatus.Pending)
            .map(assertEquals(_, None))
        }
    }
  }

  test("delete removes the entity and returns true, and get then returns None") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            created <- store.create("cust-123", 4999)
            deleted <- store.delete(created.id)
            found <- store.get(created.id)
          } yield {
            assert(deleted)
            assertEquals(found, None)
          }
        }
    }
  }

  test("delete returns false for an unknown id") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          store
            .delete(java.util.UUID.randomUUID().toString)
            .map(deleted => assert(!deleted))
        }
    }
  }

  test("delete returns false for a malformed (non-UUID) id") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          store.delete("not-a-uuid").map(deleted => assert(!deleted))
        }
    }
  }

  test("ping returns true against a real, reachable database") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          store.ping.map(assert(_))
        }
    }
  }

  test("ping returns false when the database is unreachable") {
    withContainers { postgres =>
      val unreachableConfig = configFor(postgres).copy(port = 1)
      OrderStore
        .postgres[IO](unreachableConfig, Meter.noop[IO])
        .use { store =>
          store.ping.map(ready => assert(!ready))
        }
    }
  }

  test(
    "create and get each record a db.client.operation.duration measurement, tagged by operation"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      Metrics.test[IO]("order-store-postgres-metrics-test").use {
        testMeter =>
          Migrations.run[IO](config) *> OrderStore
            .postgres[IO](config, testMeter.meter)
            .use { store =>
              for {
                created <- store.create("cust-123", 4999)
                _ <- store.get(created.id)
                metrics <- testMeter.collectMetrics
              } yield {
                val data =
                  metrics.find(_.getName == "db.client.operation.duration")
                assert(
                  data.isDefined,
                  s"expected a db.client.operation.duration series, got: $metrics"
                )
                val dbOperationKey =
                  io.opentelemetry.api.common.AttributeKey
                    .stringKey("db.operation")
                val operations = data.get.getHistogramData.getPoints.asScala
                  .flatMap(point =>
                    Option(point.getAttributes.get(dbOperationKey))
                  )
                  .toSet
                assert(
                  operations
                    .contains("insert") && operations.contains("select"),
                  s"expected db.operation attributes for both insert and select, got: $operations"
                )
              }
            }
      }
    }
  }

  test(
    "a failing query records a db.client.operation.duration measurement tagged with error.type"
  ) {
    withContainers { postgres =>
      // Port 1 is a privileged port nothing binds to in these tests; unlike
      // `mappedPort(5432) + 1`, it can't collide with another concurrently-running
      // Testcontainers Postgres instance's dynamically assigned port.
      val unreachableConfig = configFor(postgres).copy(port = 1)
      Metrics.test[IO]("order-store-postgres-metrics-test").use {
        testMeter =>
          OrderStore
            .postgres[IO](unreachableConfig, testMeter.meter)
            .use { store =>
              for {
                result <- store.create("cust-123", 4999).attempt
                metrics <- testMeter.collectMetrics
              } yield {
                assert(
                  result.isLeft,
                  s"expected the connection failure to propagate, got: $result"
                )
                val data =
                  metrics.find(_.getName == "db.client.operation.duration")
                assert(
                  data.isDefined,
                  s"expected a db.client.operation.duration series, got: $metrics"
                )
                val errorTypeKey =
                  io.opentelemetry.api.common.AttributeKey.stringKey(
                    "error.type"
                  )
                val hasErrorAttribute =
                  data.get.getHistogramData.getPoints.asScala
                    .exists(point =>
                      Option(point.getAttributes.get(errorTypeKey)).isDefined
                    )
                assert(
                  hasErrorAttribute,
                  s"expected a point tagged with error.type, got: ${data.get.getHistogramData.getPoints}"
                )
              }
            }
      }
    }
  }

  test(
    "full CRUD lifecycle: create -> read -> patch -> delete -> read-404, plus a readiness check"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            ready <- store.ping
            created <- store.create("cust-123", 4999)
            read1 <- store.get(created.id)
            updated <- store.update(created.id, OrderStatus.Reserved)
            read2 <- store.get(created.id)
            deleted <- store.delete(created.id)
            read3 <- store.get(created.id)
          } yield {
            assert(ready, "expected the database to be ready")
            assertEquals(read1, Some(created))
            assertEquals(updated.map(_.customerId), Some("cust-123"))
            assertEquals(updated.map(_.totalCents), Some(4999))
            assertEquals(updated.map(_.status), Some(OrderStatus.Reserved))
            assertEquals(read2, updated)
            assert(deleted, "expected delete to report the entity existed")
            assertEquals(read3, None)
          }
        }
    }
  }
}
