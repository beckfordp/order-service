package orderservice

import cats.effect.IO
import io.circe.syntax._
import munit.CatsEffectSuite
import org.http4s.circe.CirceEntityCodec._
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}
import org.typelevel.log4cats.noop.NoOpLogger
import purekafka.docs.AsyncApiChannel
import purerest.metrics.Metrics
import purerest.tracing.Tracing

class AppRoutesSuite extends CatsEffectSuite {

  final case class DemoPayload(message: String)
  object DemoPayload {
    implicit val schema: sttp.tapir.Schema[DemoPayload] =
      sttp.tapir.Schema.derived
  }

  private val alwaysSucceedsInventoryClient: InventoryClient[IO] =
    CheckoutFixtures.alwaysReserves

  private val noOpHistoryCache: OrderHistoryCache[IO] =
    new OrderHistoryCache[IO] {
      def get(customerId: String): IO[Option[List[OrderResponse]]] =
        IO.pure(None)
      def set(customerId: String, value: List[OrderResponse]): IO[Unit] =
        IO.unit
    }

  private val demoChannels: List[AsyncApiChannel[?]] = List(
    AsyncApiChannel[DemoPayload](
      topic = "app-routes-suite-topic",
      eventType = "AppRoutesSuiteDemoEvent"
    )
  )

  private def withRoutes[A](f: org.http4s.HttpRoutes[IO] => IO[A]): IO[A] =
    (for {
      testTracer <- Tracing.test[IO]("app-routes-suite")
      testMeter <- Metrics.test[IO]("app-routes-suite")
      store <- cats.effect.Resource.eval(OrderStore.inMemory[IO])
    } yield AppRoutes.all[IO](
      store,
      alwaysSucceedsInventoryClient,
      CheckoutFixtures.alwaysAuthorizes,
      noOpHistoryCache,
      Telemetry(testTracer.tracer, testMeter.meter, NoOpLogger[IO]),
      demoChannels,
      "target/asyncapi-docs"
    )).use(f)

  test("GET /docs (Swagger UI) resolves, not a 404") {
    withRoutes { routes =>
      routes.orNotFound
        .run(Request[IO](Method.GET, uri"/docs"))
        .map(response => assertNotEquals(response.status, Status.NotFound))
    }
  }

  test(
    "GET /docs/asyncapi.json resolves and contains the registered channel names"
  ) {
    withRoutes { routes =>
      for {
        response <- routes.orNotFound.run(
          Request[IO](Method.GET, uri"/docs/asyncapi.json")
        )
        body <- response.bodyText.compile.string
      } yield {
        assertEquals(response.status, Status.Ok)
        assert(
          body.contains("AppRoutesSuiteDemoEvent"),
          s"expected the registered channel's eventType in the AsyncAPI doc, got: $body"
        )
      }
    }
  }

  test("POST /orders resolves and creates an order end-to-end") {
    val createRequest = CheckoutFixtures.createRequest()
    withRoutes { routes =>
      routes.orNotFound
        .run(
          Request[IO](Method.POST, uri"/orders").withEntity(
            createRequest.asJson
          )
        )
        .map(response => assertEquals(response.status, Status.Created))
    }
  }
}
