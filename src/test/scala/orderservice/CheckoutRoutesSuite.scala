package orderservice

import cats.effect.{IO, Ref}
import io.circe.Json
import io.circe.syntax._
import munit.CatsEffectSuite
import org.http4s.circe.CirceEntityCodec._
import org.http4s.implicits._
import org.http4s.{Method, Request, Response, Status}
import org.typelevel.log4cats.noop.NoOpLogger

import CheckoutFixtures._

/** `POST /orders` under ADR 0018: reserve → authorize → persist, with no order
  * row on any failure and a release after every post-reserve failure.
  */
class CheckoutRoutesSuite extends CatsEffectSuite {

  private val noOpHistoryCache: OrderHistoryCache[IO] =
    new OrderHistoryCache[IO] {
      def get(customerId: String): IO[Option[List[OrderResponse]]] =
        IO.pure(None)
      def set(customerId: String, history: List[OrderResponse]): IO[Unit] =
        IO.unit
    }

  private final case class Outcome(
      response: Response[IO],
      body: Json,
      calls: List[Call],
      rows: List[(Order, List[OrderItem])]
  )

  private def checkout(
      inventory: InventoryClient[IO] = alwaysReserves,
      payment: PaymentClient[IO] = alwaysAuthorizes,
      request: CreateOrderRequest = createRequest(),
      storeOverride: Option[OrderStore[IO]] = None
  ): IO[Outcome] =
    for {
      store <- storeOverride.fold(OrderStore.inMemory[IO])(IO.pure)
      calls <- Ref.of[IO, List[Call]](Nil)
      (inv, pay) = recording(calls, inventory, payment)
      routes = OrderRoutes
        .routes[IO](store, NoOpLogger[IO], inv, pay, noOpHistoryCache)
      response <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(request)
      )
      body <- response.as[Json]
      made <- calls.get
      rows <- store.listByCustomer(request.customerId)
    } yield Outcome(response, body, made, rows)

  private def error(message: String) = Json.obj("error" -> message.asJson)

  private def releasedOnly(calls: List[Call]) = calls.collect {
    case r: Release => r
  }

  test(
    "201: reserves every line once, authorizes the total, persists as authorized"
  ) {
    val request = createRequest(
      List(
        CreateOrderItemRequest("sku-1", "Widget", 999, 2),
        CreateOrderItemRequest("sku-2", "Gadget", 500, 1)
      )
    )
    checkout(request = request).flatMap { o =>
      IO.fromEither(o.body.as[OrderResponse]).map { entity =>
        assertEquals(o.response.status, Status.Created)
        assertEquals(entity.status, "authorized")
        assertEquals(entity.totalCents, 2498)
        assertEquals(entity.shippingAddress, Some(shippingAddress))
        assert(isUuid(entity.id), entity.id)
        assertEquals(o.body.hcursor.downField("reservationFailure").focus, None)
        assertEquals(
          o.calls,
          List(
            Reserve(
              entity.id,
              entity.items
                .map(i => OrderReservationLine(i.id, i.sku, i.quantity))
            ),
            Authorize(entity.id, 2498, paymentMethodId)
          )
        )
        assertEquals(o.rows.map(_._1.id), List(entity.id))
        assertEquals(o.rows.map(_._1.status), List(OrderStatus.Authorized))
      }
    }
  }

  test("409: every failed line, no authorize, no release, no row") {
    val rejected = OrderReservationResult.Rejected(
      List(
        StockFailure("i1", "sku-1", "insufficient_stock"),
        StockFailure("i2", "sku-9", "unknown_sku")
      )
    )
    checkout(inventory = inventoryClient(reserve = IO.pure(rejected))).map {
      o =>
        assertEquals(o.response.status, Status.Conflict)
        assertEquals(
          o.body,
          Json.obj(
            "error" -> "Insufficient stock".asJson,
            "reservationFailures" -> Json.arr(
              Json.obj(
                "sku" -> "sku-1".asJson,
                "reason" -> "insufficient_stock".asJson
              ),
              Json.obj(
                "sku" -> "sku-9".asJson,
                "reason" -> "unknown_sku".asJson
              )
            )
          )
        )
        assertEquals(o.calls.collect { case r: Reserve => r }.size, 1)
        assertEquals(o.calls.size, 1)
        assertEquals(o.rows, Nil)
    }
  }

  test(
    "503 inventory: reserve raises, releases defensively, no authorize, no row"
  ) {
    checkout(inventory =
      inventoryClient(reserve = IO.raiseError(new RuntimeException("down")))
    ).map { o =>
      assertEquals(o.response.status, Status.ServiceUnavailable)
      assertEquals(o.body, error("Inventory service unavailable"))
      assertEquals(o.calls.collect { case a: Authorize => a }, Nil)
      assertEquals(releasedOnly(o.calls).size, 1)
      assertEquals(o.rows, Nil)
    }
  }

  test("402: decline releases the reservation and persists nothing") {
    checkout(payment =
      paymentClient(IO.pure(AuthorizationResult.Declined("insufficient_funds")))
    ).map { o =>
      assertEquals(o.response.status, Status.PaymentRequired)
      assertEquals(
        o.body,
        Json.obj(
          "error" -> "Payment declined".asJson,
          "paymentFailure" -> Json.obj("reason" -> "insufficient_funds".asJson)
        )
      )
      val reserved = o.calls.collect { case r: Reserve => r.orderId }
      assertEquals(releasedOnly(o.calls).map(_.orderId), reserved)
      assertEquals(o.rows, Nil)
    }
  }

  test("400: unknown payment method releases and persists nothing") {
    checkout(payment =
      paymentClient(IO.pure(AuthorizationResult.UnknownPaymentMethod))
    )
      .map { o =>
        assertEquals(o.response.status, Status.BadRequest)
        assertEquals(o.body, error("Payment method not found"))
        assertEquals(releasedOnly(o.calls).size, 1)
        assertEquals(o.rows, Nil)
      }
  }

  test("503 payment: authorize raises, releases, persists nothing") {
    checkout(payment =
      paymentClient(IO.raiseError(new RuntimeException("open")))
    )
      .map { o =>
        assertEquals(o.response.status, Status.ServiceUnavailable)
        assertEquals(o.body, error("Payment service unavailable"))
        assertEquals(releasedOnly(o.calls).size, 1)
        assertEquals(o.rows, Nil)
      }
  }

  test("500: persist failure after authorize releases the reservation") {
    for {
      base <- OrderStore.inMemory[IO]
      failing = new OrderStore[IO] {
        export base.{createAuthorized => _, *}
        def createAuthorized(
            order: Order,
            items: List[OrderItem]
        ): IO[(Order, List[OrderItem])] =
          IO.raiseError(new RuntimeException("db down"))
      }
      o <- checkout(storeOverride = Some(failing))
    } yield {
      assertEquals(o.response.status, Status.InternalServerError)
      assertEquals(o.body, error("Order could not be saved"))
      assertEquals(releasedOnly(o.calls).size, 1)
      assertEquals(o.rows, Nil)
    }
  }

  test("a failing release never changes the checkout's response") {
    checkout(
      inventory =
        inventoryClient(release = IO.raiseError(new RuntimeException("down"))),
      payment =
        paymentClient(IO.pure(AuthorizationResult.Declined("expired_card")))
    ).map { o =>
      assertEquals(o.response.status, Status.PaymentRequired)
      assertEquals(releasedOnly(o.calls).size, 1)
    }
  }

  List(
    "empty items" -> (
      createRequest(items = Nil),
      "Order must have at least one item"
    ),
    "non-positive quantity" -> (
      createRequest(List(CreateOrderItemRequest("sku-1", "Widget", 999, 0))),
      "quantity must be positive, got 0 for sku 'sku-1'"
    ),
    "blank shipping line1" -> (
      createRequest().copy(shippingAddress = shippingAddress.copy(line1 = " ")),
      "line1 is required"
    ),
    "bad shipping country" -> (
      createRequest().copy(shippingAddress =
        shippingAddress.copy(country = "UK1")
      ),
      "country must be an ISO 3166-1 alpha-2 code"
    ),
    "non-UUID paymentMethodId" -> (
      createRequest().copy(paymentMethodId = "pm-1"),
      "paymentMethodId must be a UUID"
    )
  ).foreach { case (label, (request, message)) =>
    test(s"400: $label is rejected before any downstream call") {
      checkout(request = request).map { o =>
        assertEquals(o.response.status, Status.BadRequest)
        assertEquals(o.body, error(message))
        assertEquals(o.calls, Nil)
        assertEquals(o.rows, Nil)
      }
    }
  }
}
