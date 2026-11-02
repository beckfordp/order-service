package orderservice

import cats.effect.IO
import io.circe.syntax._
import munit.CatsEffectSuite
import orderservice.domain.{
  OrderId,
  Reconciliation,
  ReconciliationStatus,
  ResolveReconciliationOutcome
}

import java.time.Instant
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

  /** A well-formed UUID that matches no stored order - the 404 cases. Using a
    * malformed string here would now be a 400 at the path codec (TD-15.1).
    */
  private val unknownOrderId = java.util.UUID.randomUUID().toString

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
      def createAuthorized(
          order: Order,
          items: List[OrderItem]
      ): IO[(Order, List[OrderItem])] =
        IO.raiseError(error)
      def get(id: OrderId): IO[Option[(Order, List[OrderItem])]] =
        IO.pure(None)
      def listByCustomer(
          customerId: String
      ): IO[List[(Order, List[OrderItem])]] = IO.raiseError(error)
      def transition(
          id: OrderId,
          event: orderservice.domain.OrderEvent,
          outboxEvent: Option[PendingOutboxEvent] = None,
          reconciliationReason: Option[String] = None
      ): IO[Either[
        orderservice.domain.TransitionRejected,
        UpdatedOrderRef
      ]] = IO.raiseError(error)
      def flagReconciliation(id: OrderId, reason: String): IO[Boolean] =
        IO.raiseError(error)
      def reconciliationReason(id: OrderId): IO[Option[String]] =
        IO.raiseError(error)
      def listAll(
          status: Option[OrderStatus],
          customerId: Option[String]
      ): IO[List[(Order, List[OrderItem])]] = IO.raiseError(error)
      def listReconciliations(
          status: Option[ReconciliationStatus],
          orderId: Option[OrderId]
      ): IO[List[Reconciliation]] = IO.raiseError(error)
      def resolveReconciliation(
          id: OrderId,
          operatorId: String,
          note: Option[String],
          resolvedAt: Instant
      ): IO[ResolveReconciliationOutcome] = IO.raiseError(error)
      def cancellationAudit(
          id: OrderId
      ): IO[Option[orderservice.domain.CancellationAudit]] =
        IO.raiseError(error)
      def delete(id: OrderId): IO[Boolean] = IO.raiseError(error)
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

  test("POST /orders stores a normalized guest email and returns it") {
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
        Request[IO](Method.POST, uri"/orders").withEntity(
          createRequest.copy(email = Some(" Guest@Example.com "))
        )
      )
      entity <- response.as[OrderResponse]
      fetched <- routes.orNotFound
        .run(Request[IO](Method.GET, uri"/orders" / entity.id))
        .flatMap(_.as[OrderResponse])
    } yield {
      assertEquals(response.status, Status.Created)
      assertEquals(entity.guestEmail, Some("guest@example.com"))
      assertEquals(fetched.guestEmail, Some("guest@example.com"))
    }
  }

  test("POST /orders rejects a malformed guest email before reserving") {
    for {
      store <- OrderStore.inMemory[IO]
      reserved <- cats.effect.Ref.of[IO, Int](0)
      inventory = new InventoryClient[IO] {
        def reserveOrder(
            orderId: String,
            items: List[OrderReservationLine]
        ): IO[OrderReservationResult] =
          reserved.update(_ + 1).as(OrderReservationResult.Reserved)
        def release(orderId: String): IO[Unit] = IO.unit
      }
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        inventory,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache
      )
      response <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(
          createRequest.copy(email = Some("not-an-email"))
        )
      )
      body <- response.as[ErrorResponse]
      calls <- reserved.get
    } yield {
      assertEquals(response.status, Status.BadRequest)
      assertEquals(body.error, "email must be a valid email address")
      assertEquals(calls, 0)
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
        Request[IO](
          Method.GET,
          uri"/orders" / java.util.UUID.randomUUID().toString
        )
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
    "GET /orders/{id} returns 400 for a malformed (non-UUID) id"
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
        Request[IO](Method.GET, uri"/orders" / "not-a-uuid")
      )
    } yield assertEquals(response.status, Status.BadRequest)
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
        Request[IO](Method.GET, uri"/orders" / unknownOrderId)
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.NotFound)
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.exists(m =>
          m.message.toLowerCase.contains("not found") &&
            m.ctx.get("order_id").contains(unknownOrderId)
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
          .withEntity(UpdateOrderRequest("cancelled"))
      )
      updated <- patchResponse.as[OrderResponse]
    } yield {
      assertEquals(patchResponse.status, Status.Ok)
      assertEquals(updated.id, created.id)
      assertEquals(updated.items, created.items)
      assertEquals(updated.status, "cancelled")
    }
  }

  test(
    "PATCH /orders/{id} publishes OrderCancelled with reason=customer"
  ) {
    for {
      storeAndOutbox <- OrderStore.inMemoryWithOutbox[IO]
      (store, outbox) = storeAndOutbox
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
      _ <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/orders" / created.id)
          .withEntity(UpdateOrderRequest("cancelled"))
      )
      repeat <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/orders" / created.id)
          .withEntity(UpdateOrderRequest("cancelled"))
      )
      events <- outbox.get
      audit <- store.cancellationAudit(
        orderservice.domain.OrderId.parse(created.id).toOption.get
      )
    } yield {
      assertEquals(repeat.status, Status.Ok)
      val cancelled = events.filter(_._2.eventType == "OrderCancelled")
      assertEquals(cancelled.size, 1)
      val event =
        cancelled.head._2.payload.as[OrderCancelledEvent].toOption.get
      assertEquals(event.reason, Some("customer"))
      assertEquals(
        event.actor,
        Some(OrderActor.customer(createRequest.customerId))
      )
      assertEquals(
        audit.map(a => (a.actorType, a.actorId, a.reason)),
        Some(("customer", createRequest.customerId, "customer"))
      )
    }
  }

  test("PATCH /orders/{id} with a valid operator token records the operator") {
    for {
      storeAndOutbox <- OrderStore.inMemoryWithOutbox[IO]
      (store, outbox) = storeAndOutbox
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache,
        OperatorAuth(Some("secret"), "op-7")
      )
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(createRequest)
      )
      created <- postResponse.as[OrderResponse]
      patchResponse <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/orders" / created.id)
          .putHeaders(
            org.http4s.headers.Authorization(
              org.http4s.Credentials.Token(
                org.http4s.AuthScheme.Bearer,
                "secret"
              )
            )
          )
          .withEntity(UpdateOrderRequest("cancelled"))
      )
      events <- outbox.get
      audit <- store.cancellationAudit(
        orderservice.domain.OrderId.parse(created.id).toOption.get
      )
    } yield {
      assertEquals(patchResponse.status, Status.Ok)
      val event =
        events
          .find(_._2.eventType == "OrderCancelled")
          .flatMap(_._2.payload.as[OrderCancelledEvent].toOption)
          .get
      assertEquals(event.reason, Some("operator"))
      assertEquals(event.actor, Some(OrderActor.operator("op-7")))
      assertEquals(
        audit.map(a => (a.actorType, a.actorId, a.reason)),
        Some(("operator", "op-7", "operator"))
      )
    }
  }

  test("PATCH /orders/{id} with an invalid operator token returns 401") {
    for {
      store <- OrderStore.inMemory[IO]
      routes = OrderRoutes.routes[IO](
        store,
        NoOpLogger[IO],
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        noOpHistoryCache,
        OperatorAuth(Some("secret"), "op-7")
      )
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(createRequest)
      )
      created <- postResponse.as[OrderResponse]
      patchResponse <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/orders" / created.id)
          .putHeaders(
            org.http4s.headers.Authorization(
              org.http4s.Credentials.Token(
                org.http4s.AuthScheme.Bearer,
                "wrong"
              )
            )
          )
          .withEntity(UpdateOrderRequest("cancelled"))
      )
      found <- store.get(
        orderservice.domain.OrderId.parse(created.id).toOption.get
      )
    } yield {
      assertEquals(patchResponse.status, Status.Unauthorized)
      assertEquals(found.map(_._1.status), Some(OrderStatus.Authorized))
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
        Request[IO](Method.PATCH, uri"/orders" / unknownOrderId)
          .withEntity(UpdateOrderRequest("cancelled"))
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
    "PATCH /orders/{id} returns 409 for a status the lifecycle does not allow"
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
          .withEntity(UpdateOrderRequest("confirmed"))
      )
      found <- store.get(
        orderservice.domain.OrderId.parse(created.id).toOption.get
      )
    } yield {
      assertEquals(patchResponse.status, Status.Conflict)
      assertEquals(found.map(_._1.status), Some(OrderStatus.Authorized))
    }
  }

  test(
    "PATCH /orders/{id} returns 409 when the order has already dispatched"
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
      orderId = orderservice.domain.OrderId.parse(created.id).toOption.get
      _ <- store.transition(
        orderId,
        orderservice.domain.OrderEvent.DispatchConfirmed
      )
      patchResponse <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/orders" / created.id)
          .withEntity(UpdateOrderRequest("cancelled"))
      )
      found <- store.get(orderId)
    } yield {
      assertEquals(patchResponse.status, Status.Conflict)
      assertEquals(found.map(_._1.status), Some(OrderStatus.Dispatched))
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
          .withEntity(UpdateOrderRequest("cancelled"))
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
        Request[IO](Method.PATCH, uri"/orders" / unknownOrderId)
          .withEntity(UpdateOrderRequest("cancelled"))
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.NotFound)
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.exists(m =>
          m.message.toLowerCase.contains("not found") &&
            m.ctx.get("order_id").contains(unknownOrderId)
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
        Request[IO](Method.DELETE, uri"/orders" / unknownOrderId)
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
        Request[IO](Method.DELETE, uri"/orders" / unknownOrderId)
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.NotFound)
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.exists(m =>
          m.message.toLowerCase.contains("not found") &&
            m.ctx.get("order_id").contains(unknownOrderId)
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
          .withEntity(UpdateOrderRequest("cancelled"))
      )
      replaced <- putResponse.as[OrderResponse]
    } yield {
      assertEquals(putResponse.status, Status.Ok)
      assertEquals(replaced.id, created.id)
      assertEquals(replaced.status, "cancelled")
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
        Request[IO](Method.PUT, uri"/orders" / unknownOrderId)
          .withEntity(UpdateOrderRequest("cancelled"))
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
        def createAuthorized(
            order: Order,
            items: List[OrderItem]
        ): IO[(Order, List[OrderItem])] =
          baseStore.createAuthorized(order, items)
        def get(id: OrderId): IO[Option[(Order, List[OrderItem])]] =
          baseStore.get(id)
        def listByCustomer(
            customerId: String
        ): IO[List[(Order, List[OrderItem])]] =
          callCount.update(_ + 1) *> baseStore.listByCustomer(customerId)
        def transition(
            id: OrderId,
            event: orderservice.domain.OrderEvent,
            outboxEvent: Option[PendingOutboxEvent] = None,
            reconciliationReason: Option[String] = None
        ): IO[Either[
          orderservice.domain.TransitionRejected,
          UpdatedOrderRef
        ]] =
          baseStore.transition(id, event, outboxEvent, reconciliationReason)
        def flagReconciliation(id: OrderId, reason: String): IO[Boolean] =
          baseStore.flagReconciliation(id, reason)
        def reconciliationReason(id: OrderId): IO[Option[String]] =
          baseStore.reconciliationReason(id)
        def listAll(
            status: Option[OrderStatus],
            customerId: Option[String]
        ): IO[List[(Order, List[OrderItem])]] =
          baseStore.listAll(status, customerId)
        def listReconciliations(
            status: Option[ReconciliationStatus],
            orderId: Option[OrderId]
        ): IO[List[Reconciliation]] =
          baseStore.listReconciliations(status, orderId)
        def resolveReconciliation(
            id: OrderId,
            operatorId: String,
            note: Option[String],
            resolvedAt: Instant
        ): IO[ResolveReconciliationOutcome] =
          baseStore.resolveReconciliation(id, operatorId, note, resolvedAt)
        def cancellationAudit(
            id: OrderId
        ): IO[Option[orderservice.domain.CancellationAudit]] =
          baseStore.cancellationAudit(id)
        def delete(id: OrderId): IO[Boolean] = baseStore.delete(id)
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

  test("GET /orders falls back to Postgres when the cache read fails") {
    val failingCache = new OrderHistoryCache[IO] {
      def get(customerId: String): IO[Option[List[OrderResponse]]] =
        IO.raiseError(new RuntimeException("redis down"))
      def set(customerId: String, value: List[OrderResponse]): IO[Unit] =
        IO.unit
    }
    for {
      store <- OrderStore.inMemory[IO]
      logger = StructuredTestingLogger.impl[IO]()
      routes = OrderRoutes.routes[IO](
        store,
        logger,
        alwaysSucceedsInventoryClient,
        alwaysAuthorizesPaymentClient,
        failingCache
      )
      _ <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/orders").withEntity(createRequest)
      )
      response <- routes.orNotFound.run(
        Request[IO](
          Method.GET,
          uri"/orders".withQueryParam("customerId", "cust-123")
        )
      )
      body <- response.as[List[OrderResponse]]
      logged <- logger.logged
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body.map(_.customerId), List("cust-123"))
      assert(
        logged.exists(_.ctx.get("cache").contains("read_error")),
        s"expected a cache read_error warning, got: $logged"
      )
    }
  }
}
