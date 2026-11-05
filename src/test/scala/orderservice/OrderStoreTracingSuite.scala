package orderservice

import cats.effect.IO
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.sdk.trace.data.SpanData
import munit.CatsEffectSuite
import orderservice.domain.{OrderEvent, OrderId, OrderItemId}
import purerest.tracing.Tracing

import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters._

class OrderStoreTracingSuite extends CatsEffectSuite {

  private val at = Instant.parse("2026-01-01T00:00:00Z")

  private def order: (Order, List[OrderItem]) = {
    val id = OrderId.fromUuid(UUID.randomUUID())
    (
      Order(id, "cust-1", 1998, OrderStatus.Authorized, at, at),
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

  private def attr(span: SpanData, key: String): Option[String] =
    Option(span.getAttributes.get(AttributeKey.stringKey(key)))

  private def named(spans: List[SpanData], name: String): List[SpanData] =
    spans.filter(_.getName == name)

  test(
    "create and transition run in business spans under the caller's span, with order.id"
  ) {
    Tracing.test[IO]("order-store-tracing").use { tt =>
      for {
        underlying <- OrderStore.inMemory[IO]
        store = OrderStoreTracing.instrument[IO](underlying, tt.tracer)
        (o, items) = order
        parentId <- tt.tracer.span("request").use { span =>
          store.createAuthorized(o, items) *>
            store.transition(o.id, OrderEvent.DispatchConfirmed) *>
            store.transition(o.id, OrderEvent.DispatchConfirmed) *>
            IO.pure(span.context.spanIdHex)
        }
        spans <- tt.finishedSpans
      } yield {
        val create = named(spans, "order.create")
        val transitions = named(spans, "order.transition")
        assertEquals(create.size, 1)
        assertEquals(transitions.size, 2)
        (create ++ transitions).foreach { s =>
          assertEquals(s.getParentSpanId, parentId)
          assertEquals(attr(s, "order.id"), Some(OrderId.render(o.id)))
        }
        val (applied, rejected) = (transitions(0), transitions(1))
        assertEquals(attr(applied, "order.event"), Some("DispatchConfirmed"))
        val events = applied.getEvents.asScala.toList
        assertEquals(events.map(_.getName), List("order.status_changed"))
        assertEquals(
          Option(events.head.getAttributes.get(AttributeKey.stringKey("to"))),
          Some("dispatched")
        )
        assertEquals(attr(applied, "outcome"), None)
        assertEquals(attr(rejected, "outcome"), Some("rejected"))
        assert(rejected.getEvents.isEmpty)
      }
    }
  }

  test("a store error marks the span Error") {
    Tracing.test[IO]("order-store-tracing-error").use { tt =>
      for {
        underlying <- OrderStore.inMemory[IO]
        failing = new OrderStore[IO] {
          export underlying.{createAuthorized => _, *}
          def createAuthorized(
              order: Order,
              items: List[OrderItem]
          ): IO[(Order, List[OrderItem])] =
            IO.raiseError(new RuntimeException("db down"))
        }
        (o, items) = order
        result <- OrderStoreTracing
          .instrument[IO](failing, tt.tracer)
          .createAuthorized(o, items)
          .attempt
        spans <- tt.finishedSpans
      } yield {
        assert(result.isLeft)
        assertEquals(
          named(spans, "order.create").map(_.getStatus.getStatusCode),
          List(StatusCode.ERROR)
        )
      }
    }
  }
}
