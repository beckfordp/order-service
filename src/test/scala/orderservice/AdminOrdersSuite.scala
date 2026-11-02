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

class AdminOrdersSuite extends CatsEffectSuite {

  private def routes(store: OrderStore[IO]): HttpRoutes[IO] =
    Http4sServerInterpreter[IO]().toRoutes(
      List(
        OrderRoutes.adminListOrdersServerEndpoint[IO](store, NoOpLogger[IO])
      )
    )

  private def seeded(
      store: OrderStore[IO],
      customerId: String,
      status: OrderStatus,
      createdAt: Instant
  ): IO[Order] = {
    val id = OrderId.fromUuid(UUID.randomUUID())
    store
      .createAuthorized(
        Order(id, customerId, 1000, status, createdAt, createdAt),
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

  test("GET /admin/orders lists every order newest-first") {
    for {
      store <- OrderStore.inMemory[IO]
      older <- seeded(
        store,
        "cust-1",
        OrderStatus.Authorized,
        Instant.parse("2026-01-01T00:00:00Z")
      )
      newer <- seeded(
        store,
        "cust-2",
        OrderStatus.Cancelled,
        Instant.parse("2026-01-02T00:00:00Z")
      )
      response <- get(routes(store), "/admin/orders")
      body <- response.as[List[OrderResponse]]
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(
        body.map(_.id),
        List(
          OrderId.render(newer.id),
          OrderId.render(older.id)
        )
      )
    }
  }

  test("GET /admin/orders filters by status and customer") {
    for {
      store <- OrderStore.inMemory[IO]
      authorized <- seeded(
        store,
        "cust-1",
        OrderStatus.Authorized,
        Instant.parse("2026-01-01T00:00:00Z")
      )
      _ <- seeded(
        store,
        "cust-2",
        OrderStatus.Cancelled,
        Instant.parse("2026-01-02T00:00:00Z")
      )
      byStatus <- get(routes(store), "/admin/orders?status=authorized")
      byStatusBody <- byStatus.as[List[OrderResponse]]
      byCustomer <- get(routes(store), "/admin/orders?customerId=cust-2")
      byCustomerBody <- byCustomer.as[List[OrderResponse]]
    } yield {
      assertEquals(
        byStatusBody.map(_.id),
        List(OrderId.render(authorized.id))
      )
      assertEquals(byCustomerBody.map(_.customerId), List("cust-2"))
    }
  }

  test("GET /admin/orders returns 400 for an unknown status") {
    for {
      store <- OrderStore.inMemory[IO]
      response <- get(routes(store), "/admin/orders?status=bogus")
    } yield assertEquals(response.status, Status.BadRequest)
  }
}
