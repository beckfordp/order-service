package orderservice

import cats.effect.IO
import io.circe.syntax._
import munit.CatsEffectSuite
import org.http4s.circe.CirceEntityCodec._
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger.{
  ERROR,
  INFO,
  WARN
}
import purerest.tracing.{ServerTracing, Tracing}

import scala.concurrent.duration._

class OrderRoutesSuite extends CatsEffectSuite {

  private val createRequest = CheckoutFixtures.createRequest()

  /** Used by every test that isn't specifically exercising reservation outcomes -
    * always reserves successfully so those tests' assertions (about logging,
    * not-found handling, etc.) aren't coupled to US-4.2.
    */
  private val alwaysSucceedsInventoryClient: InventoryClient[IO] =
    CheckoutFixtures.alwaysReserves

  private val alwaysAuthorizesPaymentClient: PaymentClient[IO] =
    CheckoutFixtures.alwaysAuthorizes

  /** Used by every test that isn't specifically exercising history/caching
    * behavior - always a miss, never actually caches anything.
    */
  private val noOpHistoryCache: OrderHistoryCache[IO] =
    new OrderHistoryCache[IO] {
      def get(customerId: String): IO[Option[List[OrderResponse]]] =
        IO.pure(None)
      def set(customerId: String, value: List[OrderResponse]): IO[Unit] =
        IO.unit
    }

  private def failingStore(error: Throwable): OrderStore[IO] =
    new OrderStore[IO] {
      def create(
          customerId: String,
          items: List[NewOrderItem]
      ): IO[(Order, List[OrderItem])] =
        IO.raiseError(error)
      def createAuthorized(
          order: Order,
          items: List[OrderItem]
      ): IO[(Order, List[OrderItem])] =
        IO.raiseError(error)
      def get(id: String): IO[Option[(Order, List[OrderItem])]] =
        IO.pure(None)
      def listByCustomer(
          customerId: String
      ): IO[List[(Order, List[OrderItem])]] = IO.raiseError(error)
      def update(
          id: String,
          status: OrderStatus,
          outboxEvent: Option[PendingOutboxEvent] = None
      ): IO[Option[(Order, List[OrderItem])]] =
        IO.raiseError(error)
      def updateStatusIfCurrent(
          id: String,
          expected: OrderStatus,
          newStatus: OrderStatus,
          outboxEvent: Option[PendingOutboxEvent] = None
      ): IO[Option[UpdatedOrderRef]] = IO.raiseError(error)
      def delete(id: String): IO[Boolean] = IO.raiseError(error)
      def ping: IO[Boolean] = IO.raiseError(error)
    }

  test("POST /orders returns 201 with the created entity and its items") {
    for {
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      request = Request[IO](Method.POST, uri"/orders").withEntity(
        createRequest
      )
      response <- routes.orNotFound.run(request)
      entity <- response.as[OrderResponse]
    } yield {
      assertEquals(response.status, Status.Created)
      assert(entity.id.nonEmpty)
      assertEquals(entity.totalCents, 999 * 5)
      assertEquals(entity.items.map(_.sku), List("sku-1"))
      assertEquals(entity.status, "authorized")
      assertEquals(
        entity.shippingAddress,
        Some(CheckoutFixtures.shippingAddress)
      )
    }
  }

  test("GET /orders/{id} returns 200 with the persisted entity") {
    for {
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(createRequest)
      )
      created <- postResponse.as[OrderResponse]
      getResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/orders" / created.id)
      )
      fetched <- getResponse.as[OrderResponse]
    } yield {
      assertEquals(getResponse.status, Status.Ok)
      assertEquals(fetched, created)
      assertEquals(fetched.items.map(_.sku), List("sku-1"))
    }
  }

  test(
    "GET /orders/{id} returns 404 with a JSON error body for an unknown id"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/orders" / "unknown-id")
      )
      body <- response.as[io.circe.Json]
    } yield {
      assertEquals(response.status, Status.NotFound)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
    }
  }

  test(
    "POST /orders logs a received-request line and a completed line with structured context"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = OrderRoutes.routes[IO](
        store,
        testLogger,
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      request = Request[IO](Method.POST, uri"/orders").withEntity(
        createRequest
      )
      response <- routes.orNotFound.run(request)
      entity <- response.as[OrderResponse]
      logged <- testLogger.logged
    } yield {
      val infos = logged.collect { case m: INFO => m }
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("received") &&
            m.ctx.get("method").contains("POST")
        ),
        s"expected a received-request INFO line with method context, got: $infos"
      )
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("completed") &&
            m.ctx.get("order_id").contains(entity.id)
        ),
        s"expected a completed INFO line with order_id context, got: $infos"
      )
    }
  }

  test(
    "POST /orders logs an ERROR with the raised throwable when persisting the entity fails"
  ) {
    val boom = new RuntimeException("boom")
    for {
      testLogger <- IO.pure(StructuredTestingLogger.impl[IO]())
      routes = OrderRoutes.routes[IO](
        failingStore(boom),
        testLogger,
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      request = Request[IO](Method.POST, uri"/orders").withEntity(
        createRequest
      )
      response <- routes.orNotFound.run(request)
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.InternalServerError)
      val errors = logged.collect { case m: ERROR => m }
      assert(
        errors.exists(m => m.throwOpt.contains(boom)),
        s"expected an ERROR line with the raised throwable, got: $errors"
      )
    }
  }

  test(
    "GET /orders/{id} logs a received-request line and a completed line for a found entity"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = OrderRoutes.routes[IO](
        store,
        testLogger,
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(createRequest)
      )
      created <- postResponse.as[OrderResponse]
      _ <- testLogger.logged // drain POST's own log lines before the GET
      getResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/orders" / created.id)
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(getResponse.status, Status.Ok)
      val infos = logged.collect { case m: INFO => m }
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("received") &&
            m.ctx.get("method").contains("GET") &&
            m.ctx.get("order_id").contains(created.id)
        ),
        s"expected a received-request INFO line with method/id context, got: $infos"
      )
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("completed") &&
            m.ctx.get("order_id").contains(created.id)
        ),
        s"expected a completed INFO line with id context, got: $infos"
      )
    }
  }

  test(
    "GET /orders/{id} logs a WARN for an unknown id"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = OrderRoutes.routes[IO](
        store,
        testLogger,
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/orders" / "unknown-id")
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.NotFound)
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.exists(m =>
          m.message.toLowerCase.contains("not found") &&
            m.ctx.get("order_id").contains("unknown-id")
        ),
        s"expected a 'not found' WARN line with id context, got: $warns"
      )
    }
  }

  test("PATCH /orders/{id} returns 200 with the updated entity and its items") {
    for {
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(createRequest)
      )
      created <- postResponse.as[OrderResponse]
      patchResponse <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/orders" / created.id)
          .withEntity(UpdateOrderRequest("pending"))
      )
      updated <- patchResponse.as[OrderResponse]
    } yield {
      assertEquals(patchResponse.status, Status.Ok)
      assertEquals(updated.id, created.id)
      assertEquals(updated.items, created.items)
    }
  }

  test(
    "PATCH /orders/{id} returns 404 with a JSON error body for an unknown id"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      response <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/orders" / "unknown-id")
          .withEntity(UpdateOrderRequest("pending"))
      )
      body <- response.as[io.circe.Json]
    } yield {
      assertEquals(response.status, Status.NotFound)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
    }
  }

  test(
    "PATCH /orders/{id} returns 400 with a JSON error body for an invalid status, and persists nothing"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(createRequest)
      )
      created <- postResponse.as[OrderResponse]
      patchResponse <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/orders" / created.id)
          .withEntity(UpdateOrderRequest("bogus"))
      )
      body <- patchResponse.as[io.circe.Json]
      getResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/orders" / created.id)
      )
      unchanged <- getResponse.as[OrderResponse]
    } yield {
      assertEquals(patchResponse.status, Status.BadRequest)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
      assertEquals(unchanged.status, "authorized")
    }
  }

  test(
    "PATCH /orders/{id} logs a WARN for an invalid status"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = OrderRoutes.routes[IO](
        store,
        testLogger,
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(createRequest)
      )
      created <- postResponse.as[OrderResponse]
      _ <- testLogger.logged // drain POST's own log lines before the PATCH
      patchResponse <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/orders" / created.id)
          .withEntity(UpdateOrderRequest("bogus"))
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(patchResponse.status, Status.BadRequest)
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.exists(m => m.message.toLowerCase.contains("invalid status")),
        s"expected an 'invalid status' WARN line, got: $warns"
      )
    }
  }

  test(
    "PUT /orders/{id} returns 400 with a JSON error body for an invalid status, and persists nothing"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(createRequest)
      )
      created <- postResponse.as[OrderResponse]
      putResponse <- routes.orNotFound.run(
        Request[IO](Method.PUT, uri"/orders" / created.id)
          .withEntity(UpdateOrderRequest("bogus"))
      )
      body <- putResponse.as[io.circe.Json]
      getResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/orders" / created.id)
      )
      unchanged <- getResponse.as[OrderResponse]
    } yield {
      assertEquals(putResponse.status, Status.BadRequest)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
      assertEquals(unchanged.status, "authorized")
    }
  }

  test(
    "PATCH /orders/{id} logs a received-request line and a completed line for a found entity"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = OrderRoutes.routes[IO](
        store,
        testLogger,
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(createRequest)
      )
      created <- postResponse.as[OrderResponse]
      _ <- testLogger.logged // drain POST's own log lines before the PATCH
      patchResponse <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/orders" / created.id)
          .withEntity(UpdateOrderRequest("pending"))
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(patchResponse.status, Status.Ok)
      val infos = logged.collect { case m: INFO => m }
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("received") &&
            m.ctx.get("method").contains("PATCH") &&
            m.ctx.get("order_id").contains(created.id)
        ),
        s"expected a received-request INFO line with method/id context, got: $infos"
      )
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("completed") &&
            m.ctx.get("order_id").contains(created.id)
        ),
        s"expected a completed INFO line with id context, got: $infos"
      )
    }
  }

  test("PATCH /orders/{id} logs a WARN for an unknown id") {
    for {
      store <- OrderStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = OrderRoutes.routes[IO](
        store,
        testLogger,
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      response <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/orders" / "unknown-id")
          .withEntity(UpdateOrderRequest("pending"))
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.NotFound)
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.exists(m =>
          m.message.toLowerCase.contains("not found") &&
            m.ctx.get("order_id").contains("unknown-id")
        ),
        s"expected a 'not found' WARN line with id context, got: $warns"
      )
    }
  }

  test(
    "DELETE /orders/{id} returns 204, and a subsequent GET returns 404"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(createRequest)
      )
      created <- postResponse.as[OrderResponse]
      deleteResponse <- routes.orNotFound.run(
        Request[IO](Method.DELETE, uri"/orders" / created.id)
      )
      getResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/orders" / created.id)
      )
    } yield {
      assertEquals(deleteResponse.status, Status.NoContent)
      assertEquals(getResponse.status, Status.NotFound)
    }
  }

  test(
    "DELETE /orders/{id} returns 404 with a JSON error body for an unknown id"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      response <- routes.orNotFound.run(
        Request[IO](Method.DELETE, uri"/orders" / "unknown-id")
      )
      body <- response.as[io.circe.Json]
    } yield {
      assertEquals(response.status, Status.NotFound)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
    }
  }

  test(
    "DELETE /orders/{id} logs a received-request line and a completed line for a found entity"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = OrderRoutes.routes[IO](
        store,
        testLogger,
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(createRequest)
      )
      created <- postResponse.as[OrderResponse]
      _ <- testLogger.logged // drain POST's own log lines before the DELETE
      deleteResponse <- routes.orNotFound.run(
        Request[IO](Method.DELETE, uri"/orders" / created.id)
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(deleteResponse.status, Status.NoContent)
      val infos = logged.collect { case m: INFO => m }
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("received") &&
            m.ctx.get("method").contains("DELETE") &&
            m.ctx.get("order_id").contains(created.id)
        ),
        s"expected a received-request INFO line with method/id context, got: $infos"
      )
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("completed") &&
            m.ctx.get("order_id").contains(created.id)
        ),
        s"expected a completed INFO line with id context, got: $infos"
      )
    }
  }

  test("DELETE /orders/{id} logs a WARN for an unknown id") {
    for {
      store <- OrderStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = OrderRoutes.routes[IO](
        store,
        testLogger,
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      response <- routes.orNotFound.run(
        Request[IO](Method.DELETE, uri"/orders" / "unknown-id")
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.NotFound)
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.exists(m =>
          m.message.toLowerCase.contains("not found") &&
            m.ctx.get("order_id").contains("unknown-id")
        ),
        s"expected a 'not found' WARN line with id context, got: $warns"
      )
    }
  }

  test("PUT /orders/{id} returns 200 with the replaced entity and its items") {
    for {
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(createRequest)
      )
      created <- postResponse.as[OrderResponse]
      putResponse <- routes.orNotFound.run(
        Request[IO](Method.PUT, uri"/orders" / created.id)
          .withEntity(UpdateOrderRequest("pending"))
      )
      replaced <- putResponse.as[OrderResponse]
    } yield {
      assertEquals(putResponse.status, Status.Ok)
      assertEquals(replaced.id, created.id)
      assertEquals(replaced.items, created.items)
    }
  }

  test(
    "PUT /orders/{id} returns 404 with a JSON error body for an unknown id"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      response <- routes.orNotFound.run(
        Request[IO](Method.PUT, uri"/orders" / "unknown-id")
          .withEntity(UpdateOrderRequest("pending"))
      )
      body <- response.as[io.circe.Json]
    } yield {
      assertEquals(response.status, Status.NotFound)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
    }
  }

  test(
    "wrapped routes (with tracing middleware) record a span for a handled request"
  ) {
    Tracing.test[IO]("order-service-test").use { testTracer =>
      for {
        store <- OrderStore.inMemory[IO]
        routes = ServerTracing.middleware(testTracer.tracer)(
          OrderRoutes.routes[IO](
            store,
            NoOpLogger[IO],
            alwaysSucceedsInventoryClient,
            alwaysAuthorizesPaymentClient,
            noOpHistoryCache
          )
        )
        request = Request[IO](Method.POST, uri"/orders").withEntity(
          createRequest
        )
        response <- routes.orNotFound.run(request)
        spans <- testTracer.finishedSpans
      } yield {
        assertEquals(response.status, Status.Created)
        assertEquals(spans.map(_.getName), List("POST /orders"))
      }
    }
  }

  test("GET /orders returns an empty list for a customer with no orders") {
    for {
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      response <- routes.orNotFound.run(
        Request[IO](
          Method.GET,
          uri"/orders".withQueryParam("customerId", "unknown-customer")
        )
      )
      body <- response.as[List[OrderResponse]]
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body, Nil)
    }
  }

  test("GET /orders returns a customer's orders newest-first") {
    for {
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      firstPost <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(createRequest)
      )
      firstOrder <- firstPost.as[OrderResponse]
      _ <- IO.sleep(2.millis)
      secondPost <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(createRequest)
      )
      secondOrder <- secondPost.as[OrderResponse]
      listResponse <- routes.orNotFound.run(
        Request[IO](
          Method.GET,
          uri"/orders".withQueryParam("customerId", "cust-123")
        )
      )
      history <- listResponse.as[List[OrderResponse]]
    } yield {
      assertEquals(listResponse.status, Status.Ok)
      assertEquals(history.map(_.id), List(secondOrder.id, firstOrder.id))
    }
  }

  test(
    "GET /orders serves from cache on a second call without querying the store again"
  ) {
    for {
      baseStore <- OrderStore.inMemory[IO]
      callCount <- IO.ref(0)
      countingStore = new OrderStore[IO] {
        def create(
            customerId: String,
            items: List[NewOrderItem]
        ): IO[(Order, List[OrderItem])] = baseStore.create(customerId, items)
        def createAuthorized(
            order: Order,
            items: List[OrderItem]
        ): IO[(Order, List[OrderItem])] =
          baseStore.createAuthorized(order, items)
        def get(id: String): IO[Option[(Order, List[OrderItem])]] =
          baseStore.get(id)
        def listByCustomer(
            customerId: String
        ): IO[List[(Order, List[OrderItem])]] =
          callCount.update(_ + 1) *> baseStore.listByCustomer(customerId)
        def update(
            id: String,
            status: OrderStatus,
            outboxEvent: Option[PendingOutboxEvent] = None
        ): IO[Option[(Order, List[OrderItem])]] =
          baseStore.update(id, status, outboxEvent)
        def updateStatusIfCurrent(
            id: String,
            expected: OrderStatus,
            newStatus: OrderStatus,
            outboxEvent: Option[PendingOutboxEvent] = None
        ): IO[Option[UpdatedOrderRef]] =
          baseStore.updateStatusIfCurrent(id, expected, newStatus, outboxEvent)
        def delete(id: String): IO[Boolean] = baseStore.delete(id)
        def ping: IO[Boolean] = baseStore.ping
      }
      cache <- OrderHistoryCache.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        countingStore,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        cache
      )
      _ <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(createRequest)
      )
      firstList <- routes.orNotFound.run(
        Request[IO](
          Method.GET,
          uri"/orders".withQueryParam("customerId", "cust-123")
        )
      )
      secondList <- routes.orNotFound.run(
        Request[IO](
          Method.GET,
          uri"/orders".withQueryParam("customerId", "cust-123")
        )
      )
      calls <- callCount.get
    } yield {
      assertEquals(firstList.status, Status.Ok)
      assertEquals(secondList.status, Status.Ok)
      assertEquals(calls, 1)
    }
  }
}
