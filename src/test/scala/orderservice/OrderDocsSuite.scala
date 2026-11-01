package orderservice

import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.circe.CirceEntityCodec._
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}
import org.typelevel.log4cats.noop.NoOpLogger
import sttp.tapir.server.ServerEndpoint
import purerest.docs.Docs
import sttp.tapir.EndpointOutput

class OrderDocsSuite extends CatsEffectSuite {

  test(
    "the tapir-described endpoint is served and documented via purerest.docs"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      inventoryClient = CheckoutFixtures.alwaysReserves
      endpoint = OrderRoutes.serverEndpoint[IO](
        store,
        NoOpLogger[IO],
        inventoryClient,
        CheckoutFixtures.alwaysAuthorizes
      )
      routes = Docs.routes[IO]("Order Service", "1.0", List(endpoint))
      request = Request[IO](Method.POST, uri"/orders")
        .withEntity(
          CheckoutFixtures.createRequest()
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

  /** Every fixed status an output can produce (tapir's own traversal is
    * internal).
    */
  private def statuses(output: EndpointOutput[?]): List[Int] =
    output match {
      case EndpointOutput.FixedStatusCode(code, _, _) => List(code.code)
      case EndpointOutput.OneOf(variants, _)          =>
        variants.flatMap(v => statuses(v.output))
      case EndpointOutput.Pair(left, right, _, _) =>
        statuses(left) ++ statuses(right)
      case EndpointOutput.MappedPair(pair, _) => statuses(pair)
      case _                                  => Nil
    }

  private def errorStatuses(endpoint: ServerEndpoint[Any, IO]): List[Int] =
    statuses(endpoint.endpoint.errorOutput).sorted

  test("each endpoint documents only the errors it can return") {
    for {
      store <- OrderStore.inMemory[IO]
      cache <- OrderHistoryCache.inMemory[IO]
    } yield {
      val logger = NoOpLogger[IO]
      val checkout = OrderRoutes.serverEndpoint[IO](
        store,
        logger,
        CheckoutFixtures.alwaysReserves,
        CheckoutFixtures.alwaysAuthorizes
      )
      val list = OrderRoutes.listOrdersServerEndpoint[IO](
        store,
        logger,
        cache
      )
      assertEquals(
        errorStatuses(checkout),
        List(400, 400, 400, 402, 409, 500, 503, 503)
      )
      assertEquals(errorStatuses(list), Nil)
      assertEquals(
        errorStatuses(OrderRoutes.getOrderServerEndpoint[IO](store, logger)),
        List(404)
      )
      assertEquals(
        errorStatuses(OrderRoutes.updateOrderServerEndpoint[IO](store, logger)),
        List(400, 404, 409)
      )
      assertEquals(
        errorStatuses(
          OrderRoutes.replaceOrderServerEndpoint[IO](store, logger)
        ),
        List(400, 404, 409)
      )
      assertEquals(
        errorStatuses(OrderRoutes.deleteOrderServerEndpoint[IO](store, logger)),
        List(404)
      )
    }
  }

  test("every endpoint is documented") {
    for {
      store <- OrderStore.inMemory[IO]
      cache <- OrderHistoryCache.inMemory[IO]
    } yield {
      val logger = NoOpLogger[IO]
      val endpoints = List(
        OrderRoutes.serverEndpoint[IO](
          store,
          logger,
          CheckoutFixtures.alwaysReserves,
          CheckoutFixtures.alwaysAuthorizes
        ),
        OrderRoutes.listOrdersServerEndpoint[IO](store, logger, cache),
        OrderRoutes.getOrderServerEndpoint[IO](store, logger),
        OrderRoutes.updateOrderServerEndpoint[IO](store, logger),
        OrderRoutes.replaceOrderServerEndpoint[IO](store, logger),
        OrderRoutes.deleteOrderServerEndpoint[IO](store, logger),
        HealthRoutes.healthServerEndpoint[IO],
        HealthRoutes.readyServerEndpoint[IO](store)
      )
      assertEquals(
        Docs.undocumented(endpoints.map(_.endpoint)).map(_.render),
        Nil
      )
    }
  }
}
