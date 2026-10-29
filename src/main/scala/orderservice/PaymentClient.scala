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

import java.time.Instant

sealed trait AuthorizationResult

object AuthorizationResult {
  final case class Authorized(authId: String, expiresAt: Instant)
      extends AuthorizationResult
  final case class Declined(reason: String) extends AuthorizationResult
  case object UnknownPaymentMethod extends AuthorizationResult
}

/** The synchronous, resilience-wrapped call to payment-service's
  * `POST /payments/authorize` (US-14.2, ADR 0011/0018; contract in
  * `gluon/docs/system-design.md`). Mirrors `InventoryClient`: business outcomes
  * are values, anything else (5xx after retries, an open breaker, a `400`)
  * raises and becomes the checkout's `503`.
  */
trait PaymentClient[F[_]] {
  def authorize(
      orderId: String,
      amountCents: Int,
      paymentMethodId: String
  ): F[AuthorizationResult]
}

object PaymentClient {

  private final case class AuthorizeBody(
      orderId: String,
      amountCents: Int,
      paymentMethodId: String
  )

  private object AuthorizeBody {
    implicit val codec: Codec[AuthorizeBody] = deriveCodec
  }

  private final case class AuthorizedBody(authId: String, expiresAt: Instant)

  private object AuthorizedBody {
    implicit val codec: Codec[AuthorizedBody] = deriveCodec
  }

  private final case class DeclinedBody(error: String, reason: String)

  private object DeclinedBody {
    implicit val codec: Codec[DeclinedBody] = deriveCodec
  }

  /** Same shape as `InventoryClient.resilient`: wraps the shared `Client[F]`
    * with this client's own resilience config, so each downstream has its own
    * breaker.
    */
  def resilient[F[_]: Async](
      httpClient: Client[F],
      config: PaymentClientConfig,
      telemetry: Telemetry[F]
  ): PaymentClient[F] =
    apply(
      Resilience.middleware[F](config.resilience)(telemetry.logger)(
        telemetry.meter
      )(httpClient),
      config.baseUrl
    )

  def apply[F[_]: Async](client: Client[F], baseUri: Uri): PaymentClient[F] =
    new PaymentClient[F] {
      def authorize(
          orderId: String,
          amountCents: Int,
          paymentMethodId: String
      ): F[AuthorizationResult] = {
        val request =
          Request[F](Method.POST, baseUri / "payments" / "authorize")
            .withEntity(AuthorizeBody(orderId, amountCents, paymentMethodId))
        client.run(request).use { response =>
          response.status match {
            case Status.Ok =>
              response
                .as[AuthorizedBody]
                .map(b => AuthorizationResult.Authorized(b.authId, b.expiresAt))
            case Status.PaymentRequired =>
              response
                .as[DeclinedBody]
                .map(b => AuthorizationResult.Declined(b.reason))
            case Status.NotFound =>
              Async[F].pure(AuthorizationResult.UnknownPaymentMethod)
            case other =>
              Async[F].raiseError(
                new RuntimeException(
                  s"Unexpected response from payment-service: $other"
                )
              )
          }
        }
      }
    }
}
