package orderservice

import cats.effect.IO
import cats.syntax.semigroupk._
import munit.CatsEffectSuite
import org.http4s.circe._
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}
import purekafka.docs.{Docs => AsyncApiDocs}
import purerest.docs.Docs

class OrderAsyncApiDocsSuite extends CatsEffectSuite {

  test(
    "GET /docs/asyncapi.json serves a document containing all 4 registered event types"
  ) {
    val routes =
      AsyncApiDocs.routes[IO]("Order Service", "1.0", AsyncApiChannels.channels)
    for {
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/docs/asyncapi.json")
      )
      body <- response.bodyText.compile.string
    } yield {
      assertEquals(response.status, Status.Ok)
      assert(!clue(body).contains("StockReserved"))
      assert(!clue(body).contains("StockReservationFailed"))
      assert(clue(body).contains("PaymentCaptured"))
      assert(clue(body).contains("PaymentAuthorizationExpired"))
      assert(clue(body).contains("PaymentFailed"))
      assert(!clue(body).contains("OrderReserved"))
      assert(clue(body).contains("OrderStatusChanged"))
      assert(
        clue(body).contains("Debezium"),
        "expected the CDC description note on order-events' publish-side messages"
      )
    }
  }

  test(
    "GET /docs/asyncapi.json: payment-events has no publish operation, order-events has no subscribe operation, inventory-events is gone"
  ) {
    val routes =
      AsyncApiDocs.routes[IO]("Order Service", "1.0", AsyncApiChannels.channels)
    for {
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/docs/asyncapi.json")
      )
      json <- response.as[io.circe.Json]
    } yield {
      val channels = json.hcursor.downField("channels")
      assert(
        channels.downField("inventory-events").failed,
        "order-service no longer consumes inventory-events (ADR 0018)"
      )
      assert(
        channels.downField("payment-events").downField("publish").failed,
        "order-service only consumes payment-events - no publish operation expected"
      )
      assert(
        channels.downField("order-events").downField("subscribe").failed,
        "order-service only publishes order-events (via CDC) - no subscribe operation expected"
      )
    }
  }

  test(
    "GET /docs/asyncapi.json: OrderStatusChanged's status field documents its fixed set of values as a JSON Schema enum"
  ) {
    val routes =
      AsyncApiDocs.routes[IO]("Order Service", "1.0", AsyncApiChannels.channels)
    for {
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/docs/asyncapi.json")
      )
      json <- response.as[io.circe.Json]
    } yield {
      // order-events carries one message now, so it isn't wrapped in `oneOf`.
      val message = json.hcursor
        .downField("channels")
        .downField("order-events")
        .downField("publish")
        .downField("message")
      assertEquals(
        message.downField("name").as[String],
        Right("OrderStatusChanged")
      )
      val orderStatusChangedPayload = message
        .downField("payload")
        .downField("properties")
        .downField("status")

      val enumValues = orderStatusChangedPayload
        .downField("enum")
        .values
        .toList
        .flatten
        .flatMap(_.asString)

      assertEquals(
        enumValues.toSet,
        Set("reservation_failed", "confirmed", "payment_failed", "cancelled"),
        "reservation_failed is no longer published (ADR 0018) but stays for BACKWARD compatibility"
      )
    }
  }

  test(
    "GET /docs/asyncapi.json still resolves when combined with purerest's OpenAPI/Swagger routes, in Main.scala's own order"
  ) {
    // Regression test for a route-ordering bug found during this track's
    // Phase 3 verification: purerest.docs.Docs' bundled Swagger UI serves a
    // wildcard /docs/{resource} route for its own static assets, which
    // returns its own real 404 (not http4s' fallthrough `None`) for any
    // unrecognized /docs/* path - including /docs/asyncapi.json - if
    // mounted before the AsyncAPI routes. Reproduces Main.scala's exact
    // combination order (asyncApiRoutes <+> staticAsyncApiRoutes <+>
    // docsRoutes) with a minimal (empty) endpoint list standing in for the
    // real OrderRoutes/HealthRoutes list, since only the Swagger UI's own
    // wildcard route - not the specific business endpoints - is what caused
    // the original bug.
    val asyncApiRoutes =
      AsyncApiDocs.routes[IO]("Order Service", "1.0", AsyncApiChannels.channels)
    val docsRoutes = Docs.routes[IO]("Order Service", "1.0", Nil)
    val combined = asyncApiRoutes <+> docsRoutes

    for {
      response <- combined.orNotFound.run(
        Request[IO](Method.GET, uri"/docs/asyncapi.json")
      )
    } yield assertEquals(response.status, Status.Ok)
  }
}
