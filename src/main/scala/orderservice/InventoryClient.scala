package orderservice

import cats.effect.Async
import cats.syntax.all._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client
import org.http4s.{Method, Request, Status, Uri}
import purerest.resilience.Resilience

final case class OrderReservationLine(
    orderItemId: String,
    sku: String,
    quantity: Int
)

object OrderReservationLine {
  implicit val codec: Codec[OrderReservationLine] = deriveCodec
}

/** One line inventory-service couldn't reserve; `reason` is its wire value
  * (`insufficient_stock` | `unknown_sku`), passed through to gshop as-is.
  */
final case class StockFailure(orderItemId: String, sku: String, reason: String)

object StockFailure {
  implicit val codec: Codec[StockFailure] = deriveCodec
}

sealed trait OrderReservationResult

object OrderReservationResult {
  case object Reserved extends OrderReservationResult
  final case class Rejected(failures: List[StockFailure])
      extends OrderReservationResult
}

/** The synchronous, resilience-wrapped calls to inventory-service's bulk
  * reservation endpoints (TD-7, ADR 0018; contracts pinned in
  * `gluon/docs/system-design.md`). `client` is expected to already be wrapped
  * with `purerest.resilience.Resilience.middleware` by the caller - this type
  * only knows how to talk to the one endpoint, not how to be resilient (see
  * README's "Calling other services with resilience").
  */
trait InventoryClient[F[_]] {

  /** All-or-nothing bulk reserve (`POST /inventorys/order-reservations`,
    * TD-7.1). A `409` naming no failed lines (an already-released order — never
    * expected, since ids are minted per checkout) raises like any other
    * unexpected response.
    */
  def reserveOrder(
      orderId: String,
      items: List[OrderReservationLine]
  ): F[OrderReservationResult]

  /** Idempotent release of everything `orderId` holds (TD-7.2). */
  def release(orderId: String): F[Unit]
}

object InventoryClient {

  private final case class ReserveOrderBody(
      orderId: String,
      items: List[OrderReservationLine]
  )

  private object ReserveOrderBody {
    implicit val codec: Codec[ReserveOrderBody] = deriveCodec
  }

  private final case class RejectedBody(
      error: String,
      failures: Option[List[StockFailure]]
  )

  private object RejectedBody {
    implicit val codec: Codec[RejectedBody] = deriveCodec
  }

  private def unexpected[F[_]: Async, A](status: Status): F[A] =
    Async[F].raiseError(
      new RuntimeException(
        s"Unexpected response from inventory-service: $status"
      )
    )

  /** Wraps `httpClient` with `purerest.resilience.Resilience.middleware` (using
    * `config.resilience` and `telemetry`'s logger/meter) and points it at
    * `config.baseUrl`, in one call — a plain (non-`Resource`) function, not a
    * `Resource[F, InventoryClient[F]]` constructor, since neither step acquires
    * anything needing release: `httpClient` is built once, shared, and released
    * by its own caller (see the `Resource`-built-module pattern in `purerest`'s
    * own `docs/functional-design-patterns-scala-haskell-category-theory.md`,
    * "Scaling tagless final without Cake" — one shared `Client[F]`, fanned out
    * into typed clients via plain constructors). Directly unit-testable against
    * a stub `Client[F]`, unlike building the real network client inline.
    */
  def resilient[F[_]: Async](
      httpClient: Client[F],
      config: InventoryClientConfig,
      telemetry: Telemetry[F]
  ): InventoryClient[F] =
    apply(
      Resilience.middleware[F](config.resilience)(telemetry.logger)(
        telemetry.meter
      )(httpClient),
      Uri.unsafeFromString(config.baseUrl)
    )

  def apply[F[_]: Async](client: Client[F], baseUri: Uri): InventoryClient[F] =
    new InventoryClient[F] {
      def reserveOrder(
          orderId: String,
          items: List[OrderReservationLine]
      ): F[OrderReservationResult] = {
        val request = Request[F](
          Method.POST,
          baseUri / "inventorys" / "order-reservations"
        ).withEntity(ReserveOrderBody(orderId, items))
        client.run(request).use { response =>
          response.status match {
            case Status.Created =>
              Async[F].pure(OrderReservationResult.Reserved)
            case Status.Conflict =>
              response.as[RejectedBody].flatMap { body =>
                body.failures.filter(_.nonEmpty) match {
                  case Some(failures) =>
                    Async[F].pure(OrderReservationResult.Rejected(failures))
                  case None =>
                    Async[F].raiseError[OrderReservationResult](
                      new RuntimeException(
                        s"inventory-service rejected order $orderId: ${body.error}"
                      )
                    )
                }
              }
            case other => unexpected[F, OrderReservationResult](other)
          }
        }
      }

      def release(orderId: String): F[Unit] =
        client
          .run(
            Request[F](
              Method.POST,
              baseUri / "inventorys" / "order-reservations" / orderId / "release"
            )
          )
          .use { response =>
            response.status match {
              case Status.NoContent => Async[F].unit
              case other            => unexpected[F, Unit](other)
            }
          }
    }
}
