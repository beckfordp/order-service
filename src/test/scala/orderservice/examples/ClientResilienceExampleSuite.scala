package orderservice.examples

import cats.effect.{IO, Ref, Resource}
import munit.CatsEffectSuite
import org.http4s.client.Client
import org.http4s.{Request, Response, Status}
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.otel4s.metrics.Meter
import pureconfig.{ConfigReader, ConfigSource}
import purerest.resilience.{
  CircuitBreakerConfig,
  CircuitBreakerOpen,
  Resilience,
  ResilienceConfig,
  RetryConfig
}

/** This template's reference service doesn't call any other service, so
  * purerest's retry + circuit-breaker middleware isn't wired into `Main`. When
  * you adapt this template to call a real downstream service, wrap its http4s
  * `Client[F]` with `Resilience.middleware` before building your own client on
  * top of it, as shown here — this test exercises that exact pattern against a
  * dummy client, so it stays correct as purerest evolves. See README's "Calling
  * other services with resilience".
  */
class ClientResilienceExampleSuite extends CatsEffectSuite {

  // purerest's resilience case classes don't derive ConfigReader themselves
  // (the library has no PureConfig dependency) - instances are derived here
  // instead, the same way a real service's config module would.
  private given ConfigReader[RetryConfig] = ConfigReader.derived
  private given ConfigReader[CircuitBreakerConfig] = ConfigReader.derived
  private given ConfigReader[ResilienceConfig] = ConfigReader.derived

  // Loaded from application.conf's "example-client" block - not read by the
  // live service, only by this example.
  private val exampleClientConfig: ResilienceConfig =
    ConfigSource.default.at("example-client").loadOrThrow[ResilienceConfig]

  private def dummyDownstreamClient(
      counter: Ref[IO, Int]
  )(behavior: Int => Status): Client[IO] =
    Client[IO] { _ =>
      Resource.eval(
        counter.updateAndGet(_ + 1).map(n => Response[IO](behavior(n)))
      )
    }

  test("a transient downstream failure is retried until it succeeds") {
    for {
      counter <- Ref.of[IO, Int](0)
      client = dummyDownstreamClient(counter)(n =>
        if (n < 3) Status.InternalServerError else Status.Ok
      )
      resilientClient = Resilience.middleware[IO](exampleClientConfig)(
        NoOpLogger[IO]
      )(Meter.noop[IO])(client)
      response <- resilientClient.run(Request[IO]()).use(IO.pure)
      attempts <- counter.get
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(attempts, 3)
    }
  }

  test("the circuit breaker opens after repeated failures and fails fast") {
    for {
      counter <- Ref.of[IO, Int](0)
      client = dummyDownstreamClient(counter)(_ => Status.InternalServerError)
      // Same PureConfig-loaded config, adjusted to isolate the circuit
      // breaker in this test: no retries, and a lower failure threshold so it
      // trips after just two calls instead of exampleClientConfig's 10.
      config = exampleClientConfig.copy(
        retry = exampleClientConfig.retry.copy(maxRetries = 0),
        circuitBreaker =
          exampleClientConfig.circuitBreaker.copy(failureThreshold = 2)
      )
      resilientClient = Resilience.middleware[IO](config)(NoOpLogger[IO])(
        Meter.noop[IO]
      )(client)
      _ <- resilientClient.run(Request[IO]()).use(IO.pure).attempt
      _ <- resilientClient.run(Request[IO]()).use(IO.pure).attempt
      openResult <- resilientClient.run(Request[IO]()).use(IO.pure).attempt
      attempts <- counter.get
    } yield {
      assertEquals(openResult, Left(CircuitBreakerOpen))
      // the third call was rejected by the open breaker before ever reaching
      // the dummy client
      assertEquals(attempts, 2)
    }
  }
}
