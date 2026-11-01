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
    "GET /docs/asyncapi.json serves a document containing all 8 registered event types"
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
      assert(!clue(body).contains("OrderStatusChanged"))
      assert(clue(body).contains("OrderConfirmed"))
      assert(clue(body).contains("OrderPaymentFailed"))
      assert(clue(body).contains("OrderCancelled"))
      assert(
        clue(body).contains("Debezium"),
        "expected the CDC description note on order-events' sent messages"
      )
    }
  }

  test(
    "GET /docs/asyncapi.json: order-service receives payment-events and fulfilment-events and sends order-events; inventory-events is gone"
  ) {
    val routes =
      AsyncApiDocs.routes[IO]("Order Service", "1.0", AsyncApiChannels.channels)
    for {
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/docs/asyncapi.json")
      )
      json <- response.as[io.circe.Json]
    } yield {
      val root = json.hcursor
      assertEquals(
        root.downField("asyncapi").as[String].toOption,
        Some("3.0.0")
      )
      assert(
        root.downField("channels").downField("inventory-events").failed,
        "order-service no longer consumes inventory-events (ADR 0018)"
      )
      val actions = root
        .downField("operations")
        .keys
        .toList
        .flatten
        .map(key =>
          key -> root
            .downField("operations")
            .downField(key)
            .downField("action")
            .as[String]
            .toOption
            .get
        )
        .toMap
      assertEquals(
        actions,
        Map(
          "payment-events" -> "receive",
          "fulfilment-events" -> "receive",
          "order-events" -> "send"
        )
      )
    }
  }

  test(
    "GET /docs/asyncapi.json: order-events sends the 3 per-outcome messages, with no status field"
  ) {
    val routes =
      AsyncApiDocs.routes[IO]("Order Service", "1.0", AsyncApiChannels.channels)
    for {
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/docs/asyncapi.json")
      )
      json <- response.as[io.circe.Json]
    } yield {
      val messages = json.hcursor
        .downField("channels")
        .downField("order-events")
        .downField("messages")
      val names = messages.keys.toList.flatten

      assertEquals(
        names.toSet,
        Set("OrderConfirmed", "OrderPaymentFailed", "OrderCancelled")
      )
      assert(
        messages
          .downField("OrderPaymentFailed")
          .downField("payload")
          .downField("properties")
          .downField("reason")
          .succeeded,
        "OrderPaymentFailed must document its reason field"
      )
      for (name <- names)
        assert(
          messages
            .downField(name)
            .downField("payload")
            .downField("properties")
            .downField("status")
            .failed,
          s"$name must not carry a status field - the split is per outcome"
        )
    }
  }

  test(
    "GET /docs/asyncapi.json: every consumed event carries a description and field examples (copied from the producer's registry artifact)"
  ) {
    val routes =
      AsyncApiDocs.routes[IO]("Order Service", "1.0", AsyncApiChannels.channels)
    for {
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/docs/asyncapi.json")
      )
      json <- response.as[io.circe.Json]
    } yield {
      val consumed = List(
        "payment-events" -> "PaymentCaptured",
        "payment-events" -> "PaymentAuthorizationExpired",
        "payment-events" -> "PaymentFailed",
        "fulfilment-events" -> "DispatchConfirmed",
        "fulfilment-events" -> "DispatchFailed"
      )
      for ((topic, event) <- consumed) {
        val payload = json.hcursor
          .downField("channels")
          .downField(topic)
          .downField("messages")
          .downField(event)
          .downField("payload")
        assert(
          payload.downField("description").as[String].isRight,
          s"$event has no description"
        )
        assert(
          payload
            .downField("properties")
            .downField("orderId")
            .downField("examples")
            .succeeded,
          s"$event.orderId has no example"
        )
      }
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
