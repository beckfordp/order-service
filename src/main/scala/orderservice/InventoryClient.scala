package orderservice

import cats.effect.Async
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client
import org.http4s.{Method, Request, Status, Uri}
import purerest.resilience.Resilience

sealed trait ReservationResult

object ReservationResult {
  case object Reserved extends ReservationResult
  case object InsufficientStock extends ReservationResult
  case object UnknownSku extends ReservationResult
}

/** The synchronous, resilience-wrapped call to inventory-service's
  * `POST /inventorys/reservations` (US-4.1's contract, pinned in
  * `gluon/docs/system-design.md`). `client` is expected to already be wrapped
  * with `purerest.resilience.Resilience.middleware` by the caller - this type
  * only knows how to talk to the one endpoint, not how to be resilient (see
  * README's "Calling other services with resilience").
  */
trait InventoryClient[F[_]] {
  def reserve(
      sku: String,
      quantity: Int,
      orderItemId: String
  ): F[ReservationResult]
}

object InventoryClient {

  private final case class ReserveRequestBody(
      sku: String,
      quantity: Int,
      orderItemId: String
  )

  private object ReserveRequestBody {
    implicit val codec: Codec[ReserveRequestBody] = deriveCodec
  }

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
      def reserve(
          sku: String,
          quantity: Int,
          orderItemId: String
      ): F[ReservationResult] = {
        val request = Request[F](
          Method.POST,
          baseUri / "inventorys" / "reservations"
        ).withEntity(ReserveRequestBody(sku, quantity, orderItemId))
        client.run(request).use { response =>
          response.status match {
            case Status.Ok       => Async[F].pure(ReservationResult.Reserved)
            case Status.NotFound => Async[F].pure(ReservationResult.UnknownSku)
            case Status.Conflict =>
              Async[F].pure(ReservationResult.InsufficientStock)
            case other =>
              Async[F].raiseError(
                new RuntimeException(
                  s"Unexpected response from inventory-service: $other"
                )
              )
          }
        }
      }
    }
}
