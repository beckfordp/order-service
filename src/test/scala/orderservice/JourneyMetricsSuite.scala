package orderservice

import cats.effect.IO
import io.opentelemetry.sdk.metrics.data.MetricData
import munit.CatsEffectSuite
import orderservice.domain.{OrderEvent, OrderId, OrderItemId}
import org.typelevel.log4cats.testing.StructuredTestingLogger
import purerest.metrics.Metrics

import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters._

class JourneyMetricsSuite extends CatsEffectSuite {

  private def order(enteredAt: Instant): (Order, List[OrderItem]) = {
    val id = OrderId.fromUuid(UUID.randomUUID())
    (
      Order(id, "cust-1", 1998, OrderStatus.Authorized, enteredAt, enteredAt),
      List(
        OrderItem(
          OrderItemId.fromUuid(UUID.randomUUID()),
          id,
          "sku-1",
          "W",
          999,
          2
        )
      )
    )
  }

  private def counterPoints(
      metrics: List[MetricData],
      name: String
  ): Map[Map[String, String], Long] =
    metrics
      .find(_.getName == name)
      .map(
        _.getLongSumData.getPoints.asScala
          .map { p =>
            p.getAttributes.asMap.asScala
              .map((k, v) => k.getKey -> v.toString)
              .toMap -> p.getValue
          }
          .toMap
      )
      .getOrElse(Map.empty)

  test(
    "counts placed orders and transitions, records time in state, logs milestones"
  ) {
    Metrics.test[IO]("journey-test").use { tm =>
      val logger = StructuredTestingLogger.impl[IO]()
      for {
        underlying <- OrderStore.inMemory[IO]
        store <- JourneyMetrics.instrument[IO](underlying, tm.meter, logger)
        now <- IO.realTimeInstant
        (o, items) = order(now.minusSeconds(120))
        _ <- store.createAuthorized(o, items)
        _ <- store.transition(o.id, OrderEvent.DispatchConfirmed)
        rejected <- store.transition(o.id, OrderEvent.DispatchConfirmed)
        metrics <- tm.collectMetrics
        logged <- logger.logged
      } yield {
        assert(rejected.isLeft)
        assertEquals(
          counterPoints(metrics, "orders.placed"),
          Map(Map.empty[String, String] -> 1L)
        )
        assertEquals(
          counterPoints(metrics, "orders.status.transitions"),
          Map(Map("from" -> "authorized", "to" -> "dispatched") -> 1L)
        )
        val tis = metrics
          .find(_.getName == "orders.status.time_in_state")
          .get
          .getHistogramData
          .getPoints
          .asScala
          .head
        assertEquals(
          tis.getAttributes.asMap.asScala
            .map((k, v) => k.getKey -> v.toString)
            .toMap,
          Map("status" -> "authorized")
        )
        assert(tis.getSum >= 120.0 && tis.getSum < 130.0, s"sum ${tis.getSum}")
        assertEquals(
          logged.toList.map(_.ctx.get("milestone")),
          List(Some("order_placed"), Some("order_dispatched"))
        )
        assert(
          logged.forall(_.ctx.get("order_id").contains(OrderId.render(o.id)))
        )
      }
    }
  }

  test("delegates reads unchanged") {
    Metrics.test[IO]("journey-test").use { tm =>
      for {
        underlying <- OrderStore.inMemory[IO]
        store <- JourneyMetrics.instrument[IO](
          underlying,
          tm.meter,
          StructuredTestingLogger.impl[IO]()
        )
        (o, items) = order(Instant.parse("2026-01-01T00:00:00Z"))
        _ <- store.createAuthorized(o, items)
        found <- store.get(o.id)
        listed <- store.listByCustomer("cust-1")
      } yield {
        assertEquals(found.map(_._1.id), Some(o.id))
        assertEquals(listed.map(_._1.id), List(o.id))
      }
    }
  }
}
