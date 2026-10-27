package orderservice

import cats.effect.{IO, Ref, Resource}
import io.circe.Json
import io.circe.syntax._
import munit.CatsEffectSuite
import org.http4s.circe.CirceEntityCodec._
import org.http4s.client.Client
import org.http4s.{Request, Response, Status, Uri}
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.otel4s.metrics.Meter
import purerest.resilience.{
  CircuitBreakerConfig,
  Resilience,
  ResilienceConfig,
  RetryConfig
}

import java.time.Instant
import scala.concurrent.duration._

class PaymentClientSuite extends CatsEffectSuite {

  private val baseUri = Uri.unsafeFromString("http://payment-service")

  private val fastConfig = ResilienceConfig(
    retry = RetryConfig(maxRetries = 2, baseDelay = 1.millis),
    circuitBreaker =
      CircuitBreakerConfig(failureThreshold = 10, resetTimeout = 1.hour)
  )

  private def client(behavior: Request[IO] => IO[Response[IO]]) =
    PaymentClient[IO](
      Resilience.middleware[IO](fastConfig)(NoOpLogger[IO])(Meter.noop[IO])(
        Client[IO](req => Resource.eval(behavior(req)))
      ),
      baseUri
    )

  private def respond(status: Status, body: Json) =
    (_: Request[IO]) => IO.pure(Response[IO](status).withEntity(body))

  test(
    "authorize posts orderId/amountCents/paymentMethodId and returns Authorized on 200"
  ) {
    val expiresAt = Instant.parse("2026-11-03T12:00:00Z")
    for {
      seen <- Ref.of[IO, List[(String, Json)]](Nil)
      result <- client { req =>
        req.as[Json].flatMap { body =>
          seen
            .update(_ :+ (req.uri.path.renderString -> body))
            .as(
              Response[IO](Status.Ok).withEntity(
                Json.obj(
                  "authId" -> "auth-1".asJson,
                  "expiresAt" -> expiresAt.asJson
                )
              )
            )
        }
      }.authorize("order-1", 2498, "pm-1")
      requests <- seen.get
    } yield {
      assertEquals(result, AuthorizationResult.Authorized("auth-1", expiresAt))
      assertEquals(
        requests,
        List(
          "/payments/authorize" -> Json.obj(
            "orderId" -> "order-1".asJson,
            "amountCents" -> 2498.asJson,
            "paymentMethodId" -> "pm-1".asJson
          )
        )
      )
    }
  }

  test("authorize returns Declined with the reason on 402") {
    client(
      respond(
        Status.PaymentRequired,
        Json.obj(
          "error" -> "Payment declined".asJson,
          "reason" -> "insufficient_funds".asJson
        )
      )
    ).authorize("order-1", 100, "pm-1")
      .map(assertEquals(_, AuthorizationResult.Declined("insufficient_funds")))
  }

  test("authorize returns UnknownPaymentMethod on 404") {
    client(
      respond(
        Status.NotFound,
        Json.obj("error" -> "Payment method not found".asJson)
      )
    ).authorize("order-1", 100, "pm-1")
      .map(assertEquals(_, AuthorizationResult.UnknownPaymentMethod))
  }

  test("authorize raises for an unexpected status") {
    client(respond(Status.BadRequest, Json.obj("error" -> "bad".asJson)))
      .authorize("order-1", 100, "pm-1")
      .attempt
      .map(r => assert(r.isLeft, r))
  }
}
