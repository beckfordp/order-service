package orderservice

import cats.effect.{IO, Ref, Resource}
import munit.CatsEffectSuite
import org.http4s.circe.CirceEntityCodec._
import org.http4s.client.Client
import org.http4s.{Method, Request, Response, Status, Uri}
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.otel4s.metrics.Meter
import purerest.metrics.Metrics
import purerest.resilience.{
  CircuitBreakerConfig,
  CircuitBreakerOpen,
  Resilience,
  ResilienceConfig,
  RetryConfig
}
import purerest.tracing.Tracing

import scala.concurrent.duration._

class InventoryClientSuite extends CatsEffectSuite {

  private val baseUri = Uri.unsafeFromString("http://inventory-service")

  private val fastConfig = ResilienceConfig(
    retry = RetryConfig(maxRetries = 2, baseDelay = 1.millis),
    circuitBreaker =
      CircuitBreakerConfig(failureThreshold = 10, resetTimeout = 1.hour)
  )

  private def stubClient(behavior: Request[IO] => Response[IO]): Client[IO] =
    Client[IO] { req => Resource.eval(IO(behavior(req))) }

  private def resilientClient(
      stub: Client[IO],
      config: ResilienceConfig = fastConfig
  ): Client[IO] =
    Resilience.middleware[IO](config)(NoOpLogger[IO])(Meter.noop[IO])(stub)

  test("reserveOrder posts every line once and returns Reserved for a 201") {
    for {
      seen <- Ref.of[IO, List[(String, io.circe.Json)]](Nil)
      stub = Client[IO] { req =>
        Resource.eval(
          req
            .as[io.circe.Json]
            .flatTap(body =>
              seen.update(_ :+ (req.uri.path.renderString -> body))
            )
            .as(Response[IO](Status.Created))
        )
      }
      result <- InventoryClient[IO](resilientClient(stub), baseUri)
        .reserveOrder("order-1", lines)
      requests <- seen.get
    } yield {
      assertEquals(result, OrderReservationResult.Reserved)
      assertEquals(
        requests,
        List(
          "/inventorys/order-reservations" -> io.circe.parser
            .parse(
              """{"orderId":"order-1","items":[{"orderItemId":"item-1","sku":"sku-1","quantity":2},{"orderItemId":"item-2","sku":"sku-2","quantity":1}]}"""
            )
            .getOrElse(io.circe.Json.Null)
        )
      )
    }
  }

  test("reserveOrder returns Rejected with every failed line for a 409") {
    val body =
      """{"error":"Insufficient stock","failures":[{"orderItemId":"item-1","sku":"sku-1","reason":"insufficient_stock"},{"orderItemId":"item-2","sku":"sku-2","reason":"unknown_sku"}]}"""
    val client = InventoryClient[IO](
      resilientClient(
        stubClient(_ =>
          Response[IO](Status.Conflict)
            .withEntity(
              io.circe.parser.parse(body).getOrElse(io.circe.Json.Null)
            )
        )
      ),
      baseUri
    )
    client
      .reserveOrder("order-1", lines)
      .map(
        assertEquals(
          _,
          OrderReservationResult.Rejected(
            List(
              StockFailure("item-1", "sku-1", "insufficient_stock"),
              StockFailure("item-2", "sku-2", "unknown_sku")
            )
          )
        )
      )
  }

  test("reserveOrder raises for a 409 that lists no failed lines") {
    val client = InventoryClient[IO](
      resilientClient(
        stubClient(_ =>
          Response[IO](Status.Conflict)
            .withEntity(
              io.circe.Json.obj(
                "error" -> io.circe.Json.fromString(
                  "Order reservation already released"
                )
              )
            )
        )
      ),
      baseUri
    )
    client.reserveOrder("order-1", lines).attempt.map(r => assert(r.isLeft, r))
  }

  test("reserveOrder raises for an unexpected status") {
    val client = InventoryClient[IO](
      resilientClient(stubClient(_ => Response[IO](Status.BadRequest))),
      baseUri
    )
    client.reserveOrder("order-1", lines).attempt.map(r => assert(r.isLeft, r))
  }

  test("release posts to the order's release path and succeeds on 204") {
    for {
      seen <- Ref.of[IO, List[(Method, String)]](Nil)
      stub = Client[IO] { req =>
        Resource.eval(
          seen
            .update(_ :+ (req.method -> req.uri.path.renderString))
            .as(Response[IO](Status.NoContent))
        )
      }
      _ <- InventoryClient[IO](resilientClient(stub), baseUri).release(
        "order-1"
      )
      requests <- seen.get
    } yield assertEquals(
      requests,
      List(Method.POST -> "/inventorys/order-reservations/order-1/release")
    )
  }

  test("release raises for an unexpected status") {
    val client = InventoryClient[IO](
      resilientClient(stubClient(_ => Response[IO](Status.BadRequest))),
      baseUri
    )
    client.release("order-1").attempt.map(r => assert(r.isLeft, r))
  }

  test("a transient downstream failure is retried until it succeeds") {
    for {
      counter <- Ref.of[IO, Int](0)
      stub = Client[IO] { _ =>
        Resource.eval(
          counter
            .updateAndGet(_ + 1)
            .map(n =>
              Response[IO](
                if (n < 3) Status.InternalServerError else Status.Created
              )
            )
        )
      }
      client = InventoryClient[IO](resilientClient(stub), baseUri)
      result <- client.reserveOrder("order-1", lines)
      attempts <- counter.get
    } yield {
      assertEquals(result, OrderReservationResult.Reserved)
      assertEquals(attempts, 3)
    }
  }

  test("the circuit breaker opens after repeated failures and fails fast") {
    for {
      counter <- Ref.of[IO, Int](0)
      failingStub = Client[IO] { _ =>
        Resource.eval(
          counter
            .updateAndGet(_ + 1)
            .as(Response[IO](Status.InternalServerError))
        )
      }
      config = fastConfig.copy(
        retry = fastConfig.retry.copy(maxRetries = 0),
        circuitBreaker = fastConfig.circuitBreaker.copy(failureThreshold = 2)
      )
      client = InventoryClient[IO](
        resilientClient(failingStub, config),
        baseUri
      )
      _ <- client.reserveOrder("order-1", lines).attempt
      _ <- client.reserveOrder("order-1", lines).attempt
      openResult <- client.reserveOrder("order-1", lines).attempt
      attempts <- counter.get
    } yield {
      assertEquals(openResult, Left(CircuitBreakerOpen))
      assertEquals(attempts, 2)
    }
  }

  private def withTelemetry[A](f: Telemetry[IO] => IO[A]): IO[A] =
    (for {
      testTracer <- Tracing.test[IO]("inventory-client-resilient-suite")
      testMeter <- Metrics.test[IO]("inventory-client-resilient-suite")
    } yield Telemetry(testTracer.tracer, testMeter.meter, NoOpLogger[IO]))
      .use(f)

  test(
    "resilient builds an InventoryClient pointed at config.baseUrl, same as manual apply(resilientClient, baseUri) wiring"
  ) {
    withTelemetry { telemetry =>
      for {
        capturedUri <- Ref.of[IO, Option[Uri]](None)
        stub = Client[IO] { req =>
          Resource.eval(
            capturedUri.set(Some(req.uri)).as(Response[IO](Status.Created))
          )
        }
        config = InventoryClientConfig(
          baseUrl = "http://inventory-service",
          resilience = fastConfig
        )
        client = InventoryClient.resilient[IO](stub, config, telemetry)
        _ <- client.reserveOrder("order-1", lines)
        uri <- capturedUri.get
      } yield assertEquals(
        uri.map(_.toString),
        Some("http://inventory-service/inventorys/order-reservations")
      )
    }
  }

  test(
    "resilient wraps the given client with Resilience.middleware(config.resilience) - a transient failure is retried, not surfaced on the first attempt"
  ) {
    withTelemetry { telemetry =>
      for {
        counter <- Ref.of[IO, Int](0)
        stub = Client[IO] { _ =>
          Resource.eval(
            counter
              .updateAndGet(_ + 1)
              .map(n =>
                Response[IO](
                  if (n < 3) Status.InternalServerError else Status.Created
                )
              )
          )
        }
        config = InventoryClientConfig(
          baseUrl = "http://inventory-service",
          resilience = fastConfig
        )
        client = InventoryClient.resilient[IO](stub, config, telemetry)
        result <- client.reserveOrder("order-1", lines)
        attempts <- counter.get
      } yield {
        assertEquals(result, OrderReservationResult.Reserved)
        assertEquals(attempts, 3)
      }
    }
  }

  private val lines = List(
    OrderReservationLine("item-1", "sku-1", 2),
    OrderReservationLine("item-2", "sku-2", 1)
  )
}
