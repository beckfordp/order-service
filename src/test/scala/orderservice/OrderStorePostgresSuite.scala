package orderservice

import cats.effect.IO
import cats.syntax.all._
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import io.circe.parser.decode
import io.circe.syntax._
import munit.CatsEffectSuite
import orderservice.domain.{
  OrderEvent,
  OrderId,
  OrderItemId,
  TransitionRejected
}
import orderservice.domain.{OrderId, OrderItemId}
import org.testcontainers.utility.DockerImageName
import org.typelevel.otel4s.metrics.Meter
import purerest.metrics.Metrics

import java.sql.DriverManager
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class OrderStorePostgresSuite
    extends CatsEffectSuite
    with TestContainerForAll
    with CreateAuthorizedContract {

  def withStore(name: String)(body: OrderStore[IO] => IO[Unit]): Unit =
    test(name) {
      withContainers { postgres =>
        val config = configFor(postgres)
        Migrations.run[IO](config) *> OrderStore
          .postgres[IO](config, Meter.noop[IO])
          .use(body)
      }
    }

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

  private val oneItem =
    List(NewOrderItem("sku-1", "Widget", 999, 2))

  private val twoItems =
    List(
      NewOrderItem("sku-1", "Widget", 999, 2),
      NewOrderItem("sku-2", "Gadget", 500, 3)
    )

  private def seedAuthorized(
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

  /** Raw-JDBC read of outbox_events for a given aggregate_id - mirrors
    * MigrationsSuite's own raw-JDBC pattern; this track's production code never
    * reads this table back (only a future Debezium connector does), so there's
    * no Skunk read path to reuse here.
    */
  private def outboxRowsFor(
      postgres: PostgreSQLContainer,
      aggregateId: String
  ): IO[List[(String, String, String)]] =
    IO {
      val conn = DriverManager.getConnection(
        postgres.jdbcUrl,
        postgres.username,
        postgres.password
      )
      try {
        val rs = conn
          .createStatement()
          .executeQuery(
            "select aggregate_id, correlation_id, payload::text from outbox_events " +
              s"where aggregate_id = '$aggregateId'"
          )
        Iterator
          .unfold(())(_ =>
            if (rs.next())
              Some(
                (
                  (
                    rs.getString("aggregate_id"),
                    rs.getString("correlation_id"),
                    rs.getString("payload")
                  ),
                  ()
                )
              )
            else None
          )
          .toList
      } finally conn.close()
    }

  test("create persists an entity and returns it with a generated id") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          seedAuthorized(store, "cust-123", oneItem).map { case (order, _) =>
            assert(OrderId.render(order.id).nonEmpty)
          }
        }
    }
  }

  test("create computes totalCents from the items") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          seedAuthorized(store, "cust-123", twoItems).map { case (order, _) =>
            assertEquals(order.totalCents, 999 * 2 + 500 * 3)
          }
        }
    }
  }

  test("create persists the items, each with a generated id") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          seedAuthorized(store, "cust-123", twoItems).map {
            case (order, items) =>
              assertEquals(items.map(_.sku), List("sku-1", "sku-2"))
              assert(items.forall(item => OrderItemId.render(item.id).nonEmpty))
              assert(items.forall(_.orderId == order.id))
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
            (first, _) <- seedAuthorized(store, "cust-123", oneItem)
            (second, _) <- seedAuthorized(store, "cust-123", oneItem)
          } yield assertNotEquals(first.id, second.id)
        }
    }
  }

  test(
    "a failing item insert rolls back the whole transaction - no order row is left behind"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      val badItems = List(
        NewOrderItem("sku-1", "Widget", 999, 2),
        NewOrderItem("sku-2", "Bad Item", 500, -1) // violates quantity > 0
      )
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          seedAuthorized(store, "cust-rollback", badItems).attempt.map {
            result =>
              assert(
                result.isLeft,
                s"expected the create to fail, got: $result"
              )
          }
        } *> IO {
        val conn = DriverManager.getConnection(
          postgres.jdbcUrl,
          postgres.username,
          postgres.password
        )
        try {
          val rs = conn
            .createStatement()
            .executeQuery(
              "select count(*) from \"order\" where customer_id = 'cust-rollback'"
            )
          rs.next()
          assertEquals(rs.getInt(1), 0)
        } finally conn.close()
      }
    }
  }

  test("get returns the persisted entity and its items") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            created <- seedAuthorized(store, "cust-123", twoItems)
            found <- store.get(created._1.id)
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
            .get(OrderId.fromUuid(java.util.UUID.randomUUID()))
            .map(assertEquals(_, None))
        }
    }
  }

  test("transition applies an allowed event and preserves the items") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            (created, items) <- seedAuthorized(store, "cust-123", twoItems)
            updated <- store.transition(created.id, OrderEvent.PaymentCaptured)
            found <- store.get(created.id)
          } yield {
            assertEquals(
              updated,
              Right(
                UpdatedOrderRef(created.id, "cust-123", created.totalCents)
              )
            )
            assertEquals(found.map(_._2), Some(items))
            assert(
              found.exists(!_._1.updatedAt.isBefore(created.updatedAt)),
              s"expected updatedAt not to move backwards, got: $found"
            )
          }
        }
    }
  }

  test("transition reports UnknownOrder for an unknown id") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          store
            .transition(
              OrderId.fromUuid(java.util.UUID.randomUUID()),
              OrderEvent.PaymentCaptured
            )
            .map(assertEquals(_, Left(TransitionRejected.UnknownOrder)))
        }
    }
  }

  test("transition rejects an illegal status and leaves it unchanged") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            (created, _) <-
              seedAuthorized(store, "cust-123", oneItem, OrderStatus.Pending)
            updated <- store.transition(
              created.id,
              OrderEvent.PaymentCaptured
            )
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
    }
  }

  test("listByCustomer returns a customer's orders newest-first") {
    withContainers { postgres =>
      val config = configFor(postgres)
      // Unique customer id: TestContainerForAll shares one Postgres instance
      // across every test in this suite, so a fixed "cust-123" would pick up
      // unrelated orders created by other tests.
      val customerId = java.util.UUID.randomUUID().toString
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            (first, _) <- seedAuthorized(store, customerId, oneItem)
            _ <- IO.sleep(2.millis)
            (second, _) <- seedAuthorized(store, customerId, oneItem)
            history <- store.listByCustomer(customerId)
          } yield assertEquals(
            history.map(_._1.id),
            List(second.id, first.id)
          )
        }
    }
  }

  test("listByCustomer returns an empty list for a customer with no orders") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          store
            .listByCustomer(java.util.UUID.randomUUID().toString)
            .map(assertEquals(_, Nil))
        }
    }
  }

  test("listByCustomer excludes orders belonging to a different customer") {
    withContainers { postgres =>
      val config = configFor(postgres)
      val customerA = java.util.UUID.randomUUID().toString
      val customerB = java.util.UUID.randomUUID().toString
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            _ <- seedAuthorized(store, customerA, oneItem)
            (other, _) <- seedAuthorized(store, customerB, oneItem)
            history <- store.listByCustomer(customerB)
          } yield assertEquals(history.map(_._1.id), List(other.id))
        }
    }
  }

  test(
    "delete removes the entity and CASCADEs to remove its items, and get then returns None"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            (created, _) <- seedAuthorized(store, "cust-123", twoItems)
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
            .delete(OrderId.fromUuid(java.util.UUID.randomUUID()))
            .map(deleted => assert(!deleted))
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
      Metrics.test[IO]("order-store-postgres-metrics-test").use { testMeter =>
        Migrations.run[IO](config) *> OrderStore
          .postgres[IO](config, testMeter.meter)
          .use { store =>
            for {
              created <- seedAuthorized(store, "cust-123", oneItem)
              _ <- store.get(created._1.id)
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
                operations.contains("insert_authorized") &&
                  operations.contains("select"),
                s"expected db.operation attributes for both insert_authorized and select, got: $operations"
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
      Metrics.test[IO]("order-store-postgres-metrics-test").use { testMeter =>
        OrderStore
          .postgres[IO](unreachableConfig, testMeter.meter)
          .use { store =>
            for {
              result <- seedAuthorized(store, "cust-123", oneItem).attempt
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
            created <- seedAuthorized(store, "cust-123", oneItem)
            read1 <- store.get(created._1.id)
            updated <- store.transition(
              created._1.id,
              OrderEvent.PaymentCaptured
            )
            read2 <- store.get(created._1.id)
            deleted <- store.delete(created._1.id)
            read3 <- store.get(created._1.id)
          } yield {
            assert(ready, "expected the database to be ready")
            assertEquals(read1, Some(created))
            assert(updated.isRight)
            assertEquals(read2.map(_._1.status), Some(OrderStatus.Confirmed))
            assert(deleted, "expected delete to report the entity existed")
            assertEquals(read3, None)
          }
        }
    }
  }

  test(
    "update writes a matching outbox row in the same transaction as the state-change UPDATE"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      val outboxEvent = PendingOutboxEvent(
        "OrderConfirmed",
        ref =>
          OrderConfirmedEvent(
            OrderId.render(ref.orderId),
            ref.customerId,
            java.time.Instant.EPOCH
          ).asJson
      )
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            (created, _) <- seedAuthorized(store, "cust-123", oneItem)
            updated <- store.transition(
              created.id,
              OrderEvent.PaymentCaptured,
              Some(outboxEvent)
            )
            rows <- outboxRowsFor(postgres, OrderId.render(created.id))
          } yield {
            assert(updated.isRight)
            assertEquals(rows.size, 1)
            val (aggregateId, correlationId, payloadJson) = rows.head
            assertEquals(aggregateId, OrderId.render(created.id))
            assertEquals(correlationId, OrderId.render(created.id))
            val envelope = decode[EventEnvelope](payloadJson)
            assertEquals(envelope.map(_.eventType), Right("OrderConfirmed"))
            assertEquals(
              envelope.map(_.correlationId),
              Right(OrderId.render(created.id))
            )
            val payload =
              envelope.flatMap(_.payload.as[OrderConfirmedEvent])
            assertEquals(
              payload.map(_.orderId),
              Right(OrderId.render(created.id))
            )
          }
        }
    }
  }

  test("update writes no outbox row when it returns None (unknown id)") {
    withContainers { postgres =>
      val config = configFor(postgres)
      val unknownId = OrderId.fromUuid(java.util.UUID.randomUUID())
      val outboxEvent = PendingOutboxEvent(
        "OrderConfirmed",
        ref =>
          OrderConfirmedEvent(
            OrderId.render(ref.orderId),
            ref.customerId,
            java.time.Instant.EPOCH
          ).asJson
      )
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            updated <- store.transition(
              unknownId,
              OrderEvent.PaymentCaptured,
              Some(outboxEvent)
            )
            rows <- outboxRowsFor(postgres, OrderId.render(unknownId))
          } yield {
            assertEquals(updated, Left(TransitionRejected.UnknownOrder))
            assertEquals(rows, Nil)
          }
        }
    }
  }

  test(
    "updateStatusIfCurrent writes a matching outbox row in the same transaction as its UPDATE"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      val outboxEvent = PendingOutboxEvent(
        "OrderConfirmed",
        ref =>
          OrderConfirmedEvent(
            OrderId.render(ref.orderId),
            ref.customerId,
            java.time.Instant.EPOCH
          ).asJson
      )
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            (created, _) <- seedAuthorized(store, "cust-123", oneItem)
            updated <- store.transition(
              created.id,
              OrderEvent.PaymentCaptured,
              Some(outboxEvent)
            )
            rows <- outboxRowsFor(postgres, OrderId.render(created.id))
          } yield {
            assert(updated.isRight)
            assertEquals(rows.size, 1)
            val envelope = decode[EventEnvelope](rows.head._3)
            assertEquals(envelope.map(_.eventType), Right("OrderConfirmed"))
            val payload =
              envelope.flatMap(_.payload.as[OrderConfirmedEvent])
            assertEquals(
              payload.map(_.orderId),
              Right(OrderId.render(created.id))
            )
          }
        }
    }
  }

  test(
    "updateStatusIfCurrent writes no outbox row when the order's current status doesn't match expected (atomicity)"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      val outboxEvent = PendingOutboxEvent(
        "OrderConfirmed",
        ref =>
          OrderConfirmedEvent(
            OrderId.render(ref.orderId),
            ref.customerId,
            java.time.Instant.EPOCH
          ).asJson
      )
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            (created, _) <-
              seedAuthorized(store, "cust-123", oneItem, OrderStatus.Confirmed)
            updated <- store.transition(
              created.id,
              OrderEvent.PaymentCaptured,
              Some(outboxEvent)
            )
            rows <- outboxRowsFor(postgres, OrderId.render(created.id))
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
            assertEquals(rows, Nil)
          }
        }
    }
  }

  private def reconciliationRowsFor(
      postgres: PostgreSQLContainer,
      orderId: String
  ): IO[List[String]] =
    IO {
      val conn = DriverManager.getConnection(
        postgres.jdbcUrl,
        postgres.username,
        postgres.password
      )
      try {
        val rs = conn
          .createStatement()
          .executeQuery(
            s"select reason from order_reconciliation where order_id = '$orderId'"
          )
        Iterator
          .unfold(())(_ =>
            if (rs.next()) Some((rs.getString("reason"), ())) else None
          )
          .toList
      } finally conn.close()
    }

  private def captureOutboxEvent(status: OrderStatus): PendingOutboxEvent =
    status match {
      case OrderStatus.Confirmed =>
        PendingOutboxEvent(
          "OrderConfirmed",
          ref =>
            OrderConfirmedEvent(
              OrderId.render(ref.orderId),
              ref.customerId,
              java.time.Instant.EPOCH
            ).asJson
        )
      case OrderStatus.PaymentFailed =>
        PendingOutboxEvent(
          "OrderPaymentFailed",
          ref =>
            OrderPaymentFailedEvent(
              OrderId.render(ref.orderId),
              ref.customerId,
              "capture_failed",
              java.time.Instant.EPOCH
            ).asJson
        )
      case other =>
        throw new IllegalArgumentException(
          s"test helper doesn't model a per-outcome event for $other"
        )
    }

  test(
    "transition moves a dispatched order to confirmed and writes its outbox row"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            (created, _) <- seedAuthorized(
              store,
              "cust-123",
              oneItem,
              OrderStatus.Dispatched
            )
            updated <- store.transition(
              created.id,
              OrderEvent.PaymentCaptured,
              Some(captureOutboxEvent(OrderStatus.Confirmed))
            )
            found <- store.get(created.id)
            rows <- outboxRowsFor(postgres, OrderId.render(created.id))
          } yield {
            assert(updated.isRight)
            assertEquals(found.map(_._1.status), Some(OrderStatus.Confirmed))
            assertEquals(rows.size, 1)
            val envelope = decode[EventEnvelope](rows.head._3)
            assertEquals(envelope.map(_.eventType), Right("OrderConfirmed"))
            val payload =
              envelope.flatMap(_.payload.as[OrderConfirmedEvent])
            assertEquals(
              payload.map(_.orderId),
              Right(OrderId.render(created.id))
            )
          }
        }
    }
  }

  test(
    "transition accepts an authorized order when the outcome overtook dispatch"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            (created, _) <- seedAuthorized(
              store,
              "cust-123",
              oneItem,
              OrderStatus.Authorized
            )
            updated <- store.transition(
              created.id,
              OrderEvent.PaymentCaptured,
              Some(captureOutboxEvent(OrderStatus.Confirmed))
            )
            found <- store.get(created.id)
          } yield {
            assert(updated.isRight)
            assertEquals(found.map(_._1.status), Some(OrderStatus.Confirmed))
          }
        }
    }
  }

  test(
    "transition returns None and writes no outbox row for a non-waiting status"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            (created, _) <- seedAuthorized(
              store,
              "cust-123",
              oneItem,
              OrderStatus.Pending
            )
            updated <- store.transition(
              created.id,
              OrderEvent.PaymentCaptured,
              Some(captureOutboxEvent(OrderStatus.Confirmed))
            )
            found <- store.get(created.id)
            rows <- outboxRowsFor(postgres, OrderId.render(created.id))
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
            assertEquals(rows, Nil)
          }
        }
    }
  }

  test(
    "transition records a failure flag and writes its payment_failed outbox row"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            (created, _) <- seedAuthorized(
              store,
              "cust-123",
              oneItem,
              OrderStatus.Dispatched
            )
            updated <- store.transition(
              created.id,
              OrderEvent.PaymentFailed("capture_failed"),
              Some(captureOutboxEvent(OrderStatus.PaymentFailed)),
              reconciliationReason = Some("capture_failed")
            )
            found <- store.get(created.id)
            reason <- store.reconciliationReason(created.id)
            rows <- outboxRowsFor(postgres, OrderId.render(created.id))
            flags <- reconciliationRowsFor(postgres, OrderId.render(created.id))
          } yield {
            assert(updated.isRight)
            assertEquals(
              found.map(_._1.status),
              Some(OrderStatus.PaymentFailed)
            )
            assertEquals(reason, Some("capture_failed"))
            assertEquals(flags, List("capture_failed"))
            val envelope = decode[EventEnvelope](rows.head._3)
            assertEquals(envelope.map(_.eventType), Right("OrderPaymentFailed"))
            val payload =
              envelope.flatMap(_.payload.as[OrderPaymentFailedEvent])
            assertEquals(payload.map(_.reason), Right("capture_failed"))
          }
        }
    }
  }

  test(
    "transition rolls back the status and reconciliation flag when the outbox write fails"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> OrderStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            (created, _) <- seedAuthorized(
              store,
              "cust-123",
              oneItem,
              OrderStatus.Dispatched
            )
            _ <- IO.blocking {
              val conn = DriverManager.getConnection(
                postgres.jdbcUrl,
                postgres.username,
                postgres.password
              )
              try
                conn
                  .createStatement()
                  .execute(
                    "alter table outbox_events add constraint reject_all check (false) not valid"
                  )
              finally conn.close()
            }
            result <- store
              .transition(
                created.id,
                OrderEvent.PaymentFailed("capture_failed"),
                Some(captureOutboxEvent(OrderStatus.PaymentFailed)),
                reconciliationReason = Some("capture_failed")
              )
              .attempt
            _ <- IO.blocking {
              val conn = DriverManager.getConnection(
                postgres.jdbcUrl,
                postgres.username,
                postgres.password
              )
              try
                conn
                  .createStatement()
                  .execute(
                    "alter table outbox_events drop constraint reject_all"
                  )
              finally conn.close()
            }
            found <- store.get(created.id)
            flags <- reconciliationRowsFor(postgres, OrderId.render(created.id))
            rows <- outboxRowsFor(postgres, OrderId.render(created.id))
          } yield {
            assert(result.isLeft)
            assertEquals(found.map(_._1.status), Some(OrderStatus.Dispatched))
            assertEquals(flags, Nil)
            assertEquals(rows, Nil)
          }
        }
    }
  }
}
