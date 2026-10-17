package orderservice

import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}
import purekafka.docs.Docs

class OrderAsyncApiDocsSuite extends CatsEffectSuite {

  test(
    "GET /docs/asyncapi.json serves a document containing all 4 registered event types"
  ) {
    val routes = Docs.routes[IO]("Order Service", "1.0", AsyncApiChannels.all)
    for {
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/docs/asyncapi.json")
      )
      body <- response.bodyText.compile.string
    } yield {
      assertEquals(response.status, Status.Ok)
      assert(clue(body).contains("StockReserved"))
      assert(clue(body).contains("StockReservationFailed"))
      assert(clue(body).contains("PaymentSettled"))
      assert(clue(body).contains("PaymentFailed"))
    }
  }
}
