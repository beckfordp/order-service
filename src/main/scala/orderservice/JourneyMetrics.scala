package orderservice

import cats.effect.{Clock, Sync}
import cats.syntax.all._
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.{BucketBoundaries, Meter}
import orderservice.domain.{
  OrderEvent,
  OrderId,
  OrderLifecycle,
  TransitionRejected
}
import purerest.logging.Milestones

/** Order-journey business telemetry (gluon PERF-4.4, 4.11), as an `OrderStore`
  * decorator: `transition` is the only status-changing API and
  * `createAuthorized` the only way an order is born, so wrapping the store sees
  * every journey step without touching the routes or consumers.
  *
  *   - `orders.placed`: orders persisted after a successful checkout
  *   - `orders.status.transitions{from,to}`: every applied transition
  *   - `orders.status.time_in_state{status}` (s): how long an order sat in the
  *     status it just left — e.g. `authorized` is the dispatch wait
  *
  * Plus a `Milestones` log line per step. Order ids go on logs only, never on
  * metric labels.
  */
object JourneyMetrics {

  /** 1s–1d: dispatch waits run from seconds (demo) to hours (backlog). */
  val timeInStateBuckets: BucketBoundaries = BucketBoundaries(
    1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0,
    21600.0, 43200.0, 86400.0
  )

  def instrument[F[_]: Sync](
      store: OrderStore[F],
      meter: Meter[F],
      logger: StructuredLogger[F]
  ): F[OrderStore[F]] =
    (
      meter.counter[Long]("orders.placed").create,
      meter.counter[Long]("orders.status.transitions").create,
      meter
        .histogram[Double]("orders.status.time_in_state")
        .withUnit("s")
        .withExplicitBucketBoundaries(timeInStateBuckets)
        .create
    ).mapN { (placed, transitions, timeInState) =>
      new OrderStore[F] {
        export store.{createAuthorized => _, transition => _, *}

        def createAuthorized(
            order: Order,
            items: List[OrderItem]
        ): F[(Order, List[OrderItem])] =
          store.createAuthorized(order, items).flatTap { _ =>
            placed.inc() *>
              Milestones.log(logger)(
                "order_placed",
                OrderId.render(order.id),
                None,
                Map("total_cents" -> order.totalCents.toString)
              )
          }

        def transition(
            id: OrderId,
            event: OrderEvent,
            outboxEvent: Option[PendingOutboxEvent],
            reconciliationReason: Option[String]
        ): F[Either[TransitionRejected, UpdatedOrderRef]] =
          store
            .transition(id, event, outboxEvent, reconciliationReason)
            .flatTap {
              case Left(_)    => Sync[F].unit
              case Right(ref) =>
                val to = OrderLifecycle.target(event).asString
                val from = ref.from.asString
                Clock[F].realTimeInstant.flatMap { now =>
                  transitions.inc(
                    Attribute("from", from),
                    Attribute("to", to)
                  ) *>
                    timeInState.record(
                      math.max(
                        0L,
                        now.toEpochMilli - ref.enteredAt.toEpochMilli
                      ) /
                        1000.0,
                      Attribute("status", from)
                    ) *>
                    Milestones.log(logger)(
                      s"order_$to",
                      OrderId.render(id),
                      None,
                      Map("from" -> from)
                    )
                }
            }
      }
    }
}

/** `checkout.outcome{outcome}` for `POST /orders` (gluon PERF-4.4). */
trait CheckoutOutcomes[F[_]] {
  def record(result: Either[OrderError, ?]): F[Unit]
}

object CheckoutOutcomes {

  /** Low-cardinality outcome label for a checkout result. */
  def classify(result: Either[OrderError, ?]): String = result match {
    case Right(_)                          => "authorized"
    case Left(PaymentDeclined(_))          => "declined"
    case Left(InsufficientStock(_))        => "out_of_stock"
    case Left(PaymentServiceUnavailable)   => "unavailable"
    case Left(InventoryServiceUnavailable) => "unavailable"
    case Left(OrderPersistFailed)          => "error"
    case Left(EmptyOrderItems | InvalidOrderItem(_) | InvalidCheckout(_)) =>
      "invalid"
    // Not raised by checkout; counted as `error` if that ever changes.
    case Left(
          OrderNotFound | InvalidStatus(_) | InvalidReconciliationStatus(_) |
          IllegalTransition(_) | Unauthorized
        ) =>
      "error"
  }

  def create[F[_]: Sync](meter: Meter[F]): F[CheckoutOutcomes[F]] =
    meter.counter[Long]("checkout.outcome").create.map { counter => result =>
      counter.inc(Attribute("outcome", classify(result)))
    }

  def noop[F[_]: Sync]: CheckoutOutcomes[F] = _ => Sync[F].unit
}
