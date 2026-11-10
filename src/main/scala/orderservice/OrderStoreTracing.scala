// SPDX-License-Identifier: MIT

package orderservice

import cats.effect.MonadCancelThrow
import cats.syntax.all._
import orderservice.domain.{
  OrderEvent,
  OrderId,
  OrderLifecycle,
  TransitionRejected
}
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.trace.Tracer

/** Order-journey business spans (gluon PERF-5.9), as an `OrderStore` decorator:
  * the same two methods `JourneyMetrics` wraps see every journey step, and each
  * is one Postgres transaction, so its span is that step's DB transaction too.
  * Wired outermost, so milestone log lines and the `outbox.write` span nest
  * inside.
  *
  *   - `order.create`: a checkout's order persisted
  *   - `order.transition{order.event}`: an `order.status_changed{from,to}`
  *     event when applied, `outcome=rejected` when not
  *
  * A raised error marks the span Error (otel4s's default finalization).
  */
object OrderStoreTracing {

  def instrument[F[_]: MonadCancelThrow](
      store: OrderStore[F],
      tracer: Tracer[F]
  ): OrderStore[F] =
    new OrderStore[F] {
      export store.{createAuthorized => _, transition => _, *}

      def createAuthorized(
          order: Order,
          items: List[OrderItem]
      ): F[(Order, List[OrderItem])] =
        tracer
          .span("order.create", Attribute("order.id", OrderId.render(order.id)))
          .surround(store.createAuthorized(order, items))

      def transition(
          id: OrderId,
          event: OrderEvent,
          outboxEvent: Option[PendingOutboxEvent],
          reconciliationReason: Option[String]
      ): F[Either[TransitionRejected, UpdatedOrderRef]] =
        tracer
          .span(
            "order.transition",
            Attribute("order.id", OrderId.render(id)),
            Attribute("order.event", event.label)
          )
          .use { span =>
            store
              .transition(id, event, outboxEvent, reconciliationReason)
              .flatTap {
                case Right(ref) =>
                  span.addEvent(
                    "order.status_changed",
                    Attribute("from", ref.from.asString),
                    Attribute("to", OrderLifecycle.target(event).asString)
                  )
                case Left(_) =>
                  span.addAttribute(Attribute("outcome", "rejected"))
              }
          }
    }
}
