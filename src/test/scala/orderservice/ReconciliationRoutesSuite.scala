package orderservice

import cats.effect.IO
import munit.CatsEffectSuite
import orderservice.domain.{OrderId, OrderItemId}
import org.http4s.circe.CirceEntityCodec._
import org.http4s.implicits._
import org.http4s.{HttpRoutes, Method, Request, Status, Uri}
import org.typelevel.log4cats.noop.NoOpLogger
import sttp.tapir.server.http4s.Http4sServerInterpreter

import java.time.Instant
import java.util.UUID

class ReconciliationRoutesSuite extends CatsEffectSuite {

  private val auth = OperatorAuth(Some("test-token"), "op-1")

  private def routes(store: OrderStore[IO]): HttpRoutes[IO] =
    Http4sServerInterpreter[IO]().toRoutes(
      List(
        ReconciliationRoutes.listServerEndpoint[IO](store, NoOpLogger[IO]),
        ReconciliationRoutes.resolveServerEndpoint[IO](
          store,
          auth,
          NoOpLogger[IO]
        )
      )
    )

  private def seededOrder(store: OrderStore[IO]): IO[Order] = {
    val id = OrderId.fromUuid(UUID.randomUUID())
    val now = Instant.parse("2026-11-02T10:00:00Z")
    store
      .createAuthorized(
        Order(
          id,
          "cust-1",
          1000,
          OrderStatus.PaymentFailed,
          now,
          now
        ),
        List(
          OrderItem(
            OrderItemId.fromUuid(UUID.randomUUID()),
            id,
            "sku-1",
            "Widget",
            1000,
            1
          )
        )
      )
      .map(_._1)
  }

  private def get(routes: HttpRoutes[IO], path: String) =
    routes.orNotFound.run(Request[IO](Method.GET, Uri.unsafeFromString(path)))

  private def resolve(
      routes: HttpRoutes[IO],
      orderId: OrderId,
      token: Option[String],
      body: ResolveReconciliationRequest
  ) = {
    val request = Request[IO](
      Method.PATCH,
      Uri.unsafeFromString(s"/reconciliations/${OrderId.render(orderId)}")
    ).withEntity(body)
    routes.orNotFound.run(
      token.fold(request)(value => request.putHeaders("Authorization" -> value))
    )
  }

  test("GET /reconciliations lists flags and filters by status") {
    for {
      store <- OrderStore.inMemory[IO]
      order <- seededOrder(store)
      _ <- store.flagReconciliation(order.id, "capture_after_void")
      response <- get(routes(store), "/reconciliations?status=open")
      body <- response.as[List[ReconciliationResponse]]
      resolvedEmpty <- get(routes(store), "/reconciliations?status=resolved")
      resolvedBody <- resolvedEmpty.as[List[ReconciliationResponse]]
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body.map(_.orderId), List(OrderId.render(order.id)))
      assertEquals(body.map(_.status), List("open"))
      assertEquals(resolvedBody, Nil)
    }
  }

  test("GET /reconciliations rejects an unknown status") {
    for {
      store <- OrderStore.inMemory[IO]
      response <- get(routes(store), "/reconciliations?status=bogus")
    } yield assertEquals(response.status, Status.BadRequest)
  }

  test("GET /reconciliations returns an empty list for a malformed orderId") {
    for {
      store <- OrderStore.inMemory[IO]
      response <- get(routes(store), "/reconciliations?orderId=not-a-uuid")
      body <- response.as[List[ReconciliationResponse]]
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body, Nil)
    }
  }

  test("PATCH /reconciliations requires the operator token") {
    for {
      store <- OrderStore.inMemory[IO]
      order <- seededOrder(store)
      _ <- store.flagReconciliation(order.id, "capture_after_void")
      noToken <- resolve(
        routes(store),
        order.id,
        None,
        ResolveReconciliationRequest("resolved")
      )
      wrongToken <- resolve(
        routes(store),
        order.id,
        Some("Bearer wrong"),
        ResolveReconciliationRequest("resolved")
      )
    } yield {
      assertEquals(noToken.status, Status.Unauthorized)
      assertEquals(wrongToken.status, Status.Unauthorized)
    }
  }

  test("PATCH /reconciliations resolves once and is idempotent") {
    for {
      store <- OrderStore.inMemory[IO]
      order <- seededOrder(store)
      _ <- store.flagReconciliation(order.id, "capture_after_void")
      first <- resolve(
        routes(store),
        order.id,
        Some("Bearer test-token"),
        ResolveReconciliationRequest("resolved", Some("refunded"))
      )
      firstBody <- first.as[ReconciliationResponse]
      second <- resolve(
        routes(store),
        order.id,
        Some("Bearer test-token"),
        ResolveReconciliationRequest("resolved", Some("later"))
      )
      secondBody <- second.as[ReconciliationResponse]
    } yield {
      assertEquals(first.status, Status.Ok)
      assertEquals(firstBody.status, "resolved")
      assertEquals(firstBody.operatorId, Some("op-1"))
      assertEquals(firstBody.note, Some("refunded"))
      assert(firstBody.resolvedAt.isDefined)
      assertEquals(second.status, Status.Ok)
      assertEquals(secondBody.note, Some("refunded"))
      assertEquals(secondBody.resolvedAt, firstBody.resolvedAt)
    }
  }

  test("PATCH /reconciliations rejects a status other than resolved") {
    for {
      store <- OrderStore.inMemory[IO]
      order <- seededOrder(store)
      _ <- store.flagReconciliation(order.id, "capture_after_void")
      response <- resolve(
        routes(store),
        order.id,
        Some("Bearer test-token"),
        ResolveReconciliationRequest("open")
      )
    } yield assertEquals(response.status, Status.BadRequest)
  }

  test("PATCH /reconciliations returns 404 for an un-flagged order") {
    for {
      store <- OrderStore.inMemory[IO]
      order <- seededOrder(store)
      response <- resolve(
        routes(store),
        order.id,
        Some("Bearer test-token"),
        ResolveReconciliationRequest("resolved")
      )
    } yield assertEquals(response.status, Status.NotFound)
  }
}
