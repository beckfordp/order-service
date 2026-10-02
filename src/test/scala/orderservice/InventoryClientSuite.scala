package orderservice

import cats.effect.{IO, Ref, Resource}
import munit.CatsEffectSuite
import org.http4s.client.Client
import org.http4s.{Method, Request, Response, Status, Uri}
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.otel4s.metrics.Meter
import purerest.resilience.{
  CircuitBreakerConfig,
  CircuitBreakerOpen,
  Resilience,
  ResilienceConfig,
  RetryConfig
}

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

  test("reserve returns Reserved for a 200 response") {
    val client =
      InventoryClient[IO](resilientClient(stubClient(_ => Response[IO](Status.Ok))), baseUri)
    client.reserve("sku-1", 2).map(assertEquals(_, ReservationResult.Reserved))
  }

  test("reserve returns InsufficientStock for a 409 response") {
    val client = InventoryClient[IO](
      resilientClient(stubClient(_ => Response[IO](Status.Conflict))),
      baseUri
    )
    client
      .reserve("sku-1", 2)
      .map(assertEquals(_, ReservationResult.InsufficientStock))
  }

  test("reserve returns UnknownSku for a 404 response") {
    val client = InventoryClient[IO](
      resilientClient(stubClient(_ => Response[IO](Status.NotFound))),
      baseUri
    )
    client.reserve("sku-1", 2).map(assertEquals(_, ReservationResult.UnknownSku))
  }

  test("reserve raises for an unexpected status (e.g. a non-retriable 400)") {
    val client = InventoryClient[IO](
      resilientClient(stubClient(_ => Response[IO](Status.BadRequest))),
      baseUri
    )
    client
      .reserve("sku-1", 2)
      .attempt
      .map(result => assert(result.isLeft, s"expected a failure, got: $result"))
  }

  test(
    "sends the sku and quantity as the JSON request body to POST /inventorys/reservations"
  ) {
    for {
      capturedRequest <- Ref.of[IO, Option[Request[IO]]](None)
      capturedBody <- Ref.of[IO, String]("")
      stub = Client[IO] { req =>
        Resource.eval(
          req.bodyText.compile.string.flatMap { body =>
            capturedRequest.set(Some(req)) *> capturedBody.set(body)
          } *> IO(Response[IO](Status.Ok))
        )
      }
      client = InventoryClient[IO](resilientClient(stub), baseUri)
      _ <- client.reserve("sku-1", 3)
      req <- capturedRequest.get
      body <- capturedBody.get
    } yield {
      assertEquals(req.map(_.method), Some(Method.POST))
      assertEquals(req.map(_.uri.path.toString), Some("/inventorys/reservations"))
      assert(body.contains("sku-1"), s"expected sku in body, got: $body")
      assert(body.contains("3"), s"expected quantity in body, got: $body")
    }
  }

  test("a transient downstream failure is retried until it succeeds") {
    for {
      counter <- Ref.of[IO, Int](0)
      stub = Client[IO] { _ =>
        Resource.eval(
          counter
            .updateAndGet(_ + 1)
            .map(n => Response[IO](if (n < 3) Status.InternalServerError else Status.Ok))
        )
      }
      client = InventoryClient[IO](resilientClient(stub), baseUri)
      result <- client.reserve("sku-1", 2)
      attempts <- counter.get
    } yield {
      assertEquals(result, ReservationResult.Reserved)
      assertEquals(attempts, 3)
    }
  }

  test("the circuit breaker opens after repeated failures and fails fast") {
    for {
      counter <- Ref.of[IO, Int](0)
      failingStub = Client[IO] { _ =>
        Resource.eval(
          counter.updateAndGet(_ + 1).as(Response[IO](Status.InternalServerError))
        )
      }
      config = fastConfig.copy(
        retry = fastConfig.retry.copy(maxRetries = 0),
        circuitBreaker = fastConfig.circuitBreaker.copy(failureThreshold = 2)
      )
      client = InventoryClient[IO](resilientClient(failingStub, config), baseUri)
      _ <- client.reserve("sku-1", 2).attempt
      _ <- client.reserve("sku-1", 2).attempt
      openResult <- client.reserve("sku-1", 2).attempt
      attempts <- counter.get
    } yield {
      assertEquals(openResult, Left(CircuitBreakerOpen))
      assertEquals(attempts, 2)
    }
  }
}
