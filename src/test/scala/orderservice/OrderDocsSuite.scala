package orderservice

import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.circe.CirceEntityCodec._
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}
import org.typelevel.log4cats.noop.NoOpLogger
import purerest.docs.Docs

class OrderDocsSuite extends CatsEffectSuite {

  test(
    "the tapir-described endpoint is served and documented via purerest.docs"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      inventoryClient = new InventoryClient[IO] {
        def reserve(sku: String, quantity: Int): IO[ReservationResult] =
          IO.pure(ReservationResult.Reserved)
      }
      endpoint = OrderRoutes.serverEndpoint[IO](store, NoOpLogger[IO], inventoryClient)
      routes = Docs.routes[IO]("Order Service", "1.0", List(endpoint))
      request = Request[IO](Method.POST, uri"/orders")
        .withEntity(
          CreateOrderRequest(
            "cust-123",
            List(CreateOrderItemRequest("sku-1", "Widget", 999, 5))
          )
        )
      response <- routes.orNotFound.run(request)
      entity <- response.as[OrderResponse]
      docsResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/docs/docs.yaml")
      )
      docsBody <- docsResponse.bodyText.compile.string
    } yield {
      assertEquals(response.status, Status.Created)
      assert(entity.id.nonEmpty)
      assertEquals(docsResponse.status, Status.Ok)
      assert(clue(docsBody).contains("/orders"))
    }
  }
}
