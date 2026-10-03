package orderservice

import cats.effect.IO
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

  private val createRequest = CreateOrderRequest(
    "cust-123",
    List(CreateOrderItemRequest("sku-1", "Widget", 999, 5))
  )

  /** Used by every test that isn't specifically exercising reservation outcomes -
    * always reserves successfully so those tests' assertions (about logging,
    * not-found handling, etc.) aren't coupled to US-4.2.
    */
  private val alwaysSucceedsInventoryClient: InventoryClient[IO] =
    new InventoryClient[IO] {
      def reserve(
          sku: String,
          quantity: Int,
          orderItemId: String
      ): IO[ReservationResult] =
        IO.pure(ReservationResult.Reserved)
    }

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

  /** Used by every test that isn't specifically exercising order.status-changed
    * publishing - never actually produces to Kafka.
    */
  private val noOpPublisher: OrderEventPublisher[IO] =
    OrderEventPublisher.noOp[IO]

  /** Captures publishStatusChanged calls into `ref` for assertions; never
    * actually produces to Kafka. publishReserved is a no-op - not this track's
    * concern.
    */
  private def capturingPublisher(
      ref: cats.effect.Ref[IO, List[OrderStatusChangedEvent]]
  ): OrderEventPublisher[IO] =
    new OrderEventPublisher[IO] {
      def publishReserved(event: OrderReservedEvent): IO[Unit] = IO.unit
      def publishStatusChanged(event: OrderStatusChangedEvent): IO[Unit] =
        ref.update(_ :+ event)
    }

  private def failingStore(error: Throwable): OrderStore[IO] =
    new OrderStore[IO] {
      def create(
          customerId: String,
          items: List[NewOrderItem]
      ): IO[(Order, List[OrderItem])] =
        IO.raiseError(error)
      def get(id: String): IO[Option[(Order, List[OrderItem])]] =
        IO.pure(None)
      def listByCustomer(
          customerId: String
      ): IO[List[(Order, List[OrderItem])]] = IO.raiseError(error)
      def update(
          id: String,
          status: OrderStatus
      ): IO[Option[(Order, List[OrderItem])]] =
        IO.raiseError(error)
      def updateStatusByItemId(
          orderItemId: String,
          newStatus: OrderStatus
      ): IO[Option[UpdatedOrderRef]] = IO.raiseError(error)
      def updateStatusIfCurrent(
          id: String,
          expected: OrderStatus,
          newStatus: OrderStatus
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
        noOpHistoryCache,
        noOpPublisher
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
      assertEquals(entity.status, "pending")
    }
  }

  test(
    "POST /orders returns 201 with status reservation_failed when an item can't be reserved, and persists all items anyway"
  ) {
    val failsSecondItem: InventoryClient[IO] =
      new InventoryClient[IO] {
        def reserve(
            sku: String,
            quantity: Int,
            orderItemId: String
        ): IO[ReservationResult] =
          if (sku == "sku-1") IO.pure(ReservationResult.Reserved)
          else IO.pure(ReservationResult.InsufficientStock)
      }
    val twoItemRequest = CreateOrderRequest(
      "cust-123",
      List(
        CreateOrderItemRequest("sku-1", "Widget", 999, 2),
        CreateOrderItemRequest("sku-2", "Gadget", 500, 1)
      )
    )
    for {
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        failsSecondItem,
        noOpHistoryCache,
        noOpPublisher
      )
      response <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(twoItemRequest)
      )
      entity <- response.as[OrderResponse]
    } yield {
      assertEquals(response.status, Status.Created)
      assertEquals(entity.status, "reservation_failed")
      assertEquals(entity.items.map(_.sku), List("sku-1", "sku-2"))
    }
  }

  test(
    "POST /orders publishes order.status-changed with reservation_failed when an item can't be reserved"
  ) {
    val failsSecondItem: InventoryClient[IO] =
      new InventoryClient[IO] {
        def reserve(
            sku: String,
            quantity: Int,
            orderItemId: String
        ): IO[ReservationResult] =
          if (sku == "sku-1") IO.pure(ReservationResult.Reserved)
          else IO.pure(ReservationResult.InsufficientStock)
      }
    val twoItemRequest = CreateOrderRequest(
      "cust-123",
      List(
        CreateOrderItemRequest("sku-1", "Widget", 999, 2),
        CreateOrderItemRequest("sku-2", "Gadget", 500, 1)
      )
    )
    for {
      store <- OrderStore.inMemory[IO]
      published <- IO.ref(List.empty[OrderStatusChangedEvent])
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        failsSecondItem,
        noOpHistoryCache,
        capturingPublisher(published)
      )
      response <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(twoItemRequest)
      )
      entity <- response.as[OrderResponse]
      events <- published.get
    } yield {
      assertEquals(entity.status, "reservation_failed")
      assertEquals(events.map(_.orderId), List(entity.id))
      assertEquals(events.map(_.customerId), List("cust-123"))
      assertEquals(events.map(_.status), List("reservation_failed"))
    }
  }

  test(
    "POST /orders does not publish order.status-changed when reservation succeeds"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      published <- IO.ref(List.empty[OrderStatusChangedEvent])
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        noOpHistoryCache,
        capturingPublisher(published)
      )
      response <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(createRequest)
      )
      entity <- response.as[OrderResponse]
      events <- published.get
    } yield {
      assertEquals(entity.status, "pending")
      assertEquals(events, Nil)
    }
  }

  test(
    "POST /orders returns 201 with status reservation_failed when the inventory client raises (e.g. circuit open)"
  ) {
    val raisingClient: InventoryClient[IO] =
      new InventoryClient[IO] {
        def reserve(
            sku: String,
            quantity: Int,
            orderItemId: String
        ): IO[ReservationResult] =
          IO.raiseError(new RuntimeException("circuit open"))
      }
    for {
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        raisingClient,
        noOpHistoryCache,
        noOpPublisher
      )
      response <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(createRequest)
      )
      entity <- response.as[OrderResponse]
    } yield {
      assertEquals(response.status, Status.Created)
      assertEquals(entity.status, "reservation_failed")
      assertEquals(entity.items.map(_.sku), List("sku-1"))
    }
  }

  test(
    "POST /orders does not call the inventory client a second time once an earlier item fails (short-circuit)"
  ) {
    for {
      callCount <- IO.ref(0)
      client = new InventoryClient[IO] {
        def reserve(
            sku: String,
            quantity: Int,
            orderItemId: String
        ): IO[ReservationResult] =
          callCount.update(_ + 1).as(ReservationResult.InsufficientStock)
      }
      twoItemRequest = CreateOrderRequest(
        "cust-123",
        List(
          CreateOrderItemRequest("sku-1", "Widget", 999, 2),
          CreateOrderItemRequest("sku-2", "Gadget", 500, 1)
        )
      )
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        client,
        noOpHistoryCache,
        noOpPublisher
      )
      _ <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(twoItemRequest)
      )
      calls <- callCount.get
    } yield assertEquals(calls, 1)
  }

  test(
    "POST /orders returns 400 with a JSON error body for an empty items list, and persists nothing"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        noOpHistoryCache,
        noOpPublisher
      )
      request = Request[IO](Method.POST, uri"/orders")
        .withEntity(CreateOrderRequest("cust-123", Nil))
      response <- routes.orNotFound.run(request)
      body <- response.as[io.circe.Json]
    } yield {
      assertEquals(response.status, Status.BadRequest)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
    }
  }

  test(
    "POST /orders returns 400 with a JSON error body for an item with non-positive quantity"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        noOpHistoryCache,
        noOpPublisher
      )
      request = Request[IO](Method.POST, uri"/orders")
        .withEntity(
          CreateOrderRequest(
            "cust-123",
            List(CreateOrderItemRequest("sku-1", "Widget", 999, 0))
          )
        )
      response <- routes.orNotFound.run(request)
      body <- response.as[io.circe.Json]
    } yield {
      assertEquals(response.status, Status.BadRequest)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
    }
  }

  test(
    "POST /orders returns 400 with a JSON error body for an item with a negative unitPriceCents"
  ) {
    for {
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        noOpHistoryCache,
        noOpPublisher
      )
      request = Request[IO](Method.POST, uri"/orders")
        .withEntity(
          CreateOrderRequest(
            "cust-123",
            List(CreateOrderItemRequest("sku-1", "Widget", -1, 1))
          )
        )
      response <- routes.orNotFound.run(request)
      body <- response.as[io.circe.Json]
    } yield {
      assertEquals(response.status, Status.BadRequest)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
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
        noOpHistoryCache,
        noOpPublisher
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
        noOpHistoryCache,
        noOpPublisher
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
        noOpHistoryCache,
        noOpPublisher
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
        noOpHistoryCache,
        noOpPublisher
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
        noOpHistoryCache,
        noOpPublisher
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
        noOpHistoryCache,
        noOpPublisher
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
        noOpHistoryCache,
        noOpPublisher
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
        noOpHistoryCache,
        noOpPublisher
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
        noOpHistoryCache,
        noOpPublisher
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
      assertEquals(unchanged.status, "pending")
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
        noOpHistoryCache,
        noOpPublisher
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
        noOpHistoryCache,
        noOpPublisher
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
      assertEquals(unchanged.status, "pending")
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
        noOpHistoryCache,
        noOpPublisher
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
        noOpHistoryCache,
        noOpPublisher
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
        noOpHistoryCache,
        noOpPublisher
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
        noOpHistoryCache,
        noOpPublisher
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
        noOpHistoryCache,
        noOpPublisher
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
        noOpHistoryCache,
        noOpPublisher
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
        noOpHistoryCache,
        noOpPublisher
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
        noOpHistoryCache,
        noOpPublisher
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
            noOpHistoryCache,
            noOpPublisher
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
        noOpHistoryCache,
        noOpPublisher
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
        noOpHistoryCache,
        noOpPublisher
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
        def get(id: String): IO[Option[(Order, List[OrderItem])]] =
          baseStore.get(id)
        def listByCustomer(
            customerId: String
        ): IO[List[(Order, List[OrderItem])]] =
          callCount.update(_ + 1) *> baseStore.listByCustomer(customerId)
        def update(
            id: String,
            status: OrderStatus
        ): IO[Option[(Order, List[OrderItem])]] =
          baseStore.update(id, status)
        def updateStatusByItemId(
            orderItemId: String,
            newStatus: OrderStatus
        ): IO[Option[UpdatedOrderRef]] =
          baseStore.updateStatusByItemId(orderItemId, newStatus)
        def updateStatusIfCurrent(
            id: String,
            expected: OrderStatus,
            newStatus: OrderStatus
        ): IO[Option[UpdatedOrderRef]] =
          baseStore.updateStatusIfCurrent(id, expected, newStatus)
        def delete(id: String): IO[Boolean] = baseStore.delete(id)
        def ping: IO[Boolean] = baseStore.ping
      }
      cache <- OrderHistoryCache.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        countingStore,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        cache,
        noOpPublisher
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
