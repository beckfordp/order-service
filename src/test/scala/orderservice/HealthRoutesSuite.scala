package orderservice

import cats.effect.IO
import munit.CatsEffectSuite
import orderservice.domain.OrderId
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}

class HealthRoutesSuite extends CatsEffectSuite {

  private val readyStore: OrderStore[IO] =
    new OrderStore[IO] {
      def createAuthorized(
          order: Order,
          items: List[OrderItem]
      ): IO[(Order, List[OrderItem])] =
        IO.raiseError(new NotImplementedError())
      def get(id: OrderId): IO[Option[(Order, List[OrderItem])]] = IO.pure(None)
      def listByCustomer(
          customerId: String
      ): IO[List[(Order, List[OrderItem])]] = IO.pure(Nil)
      def transition(
          id: OrderId,
          event: orderservice.domain.OrderEvent,
          outboxEvent: Option[PendingOutboxEvent] = None,
          reconciliationReason: Option[String] = None
      ): IO[Either[
        orderservice.domain.TransitionRejected,
        UpdatedOrderRef
      ]] =
        IO.pure(Left(orderservice.domain.TransitionRejected.UnknownOrder))
      def flagReconciliation(id: OrderId, reason: String): IO[Boolean] =
        IO.pure(false)
      def reconciliationReason(id: OrderId): IO[Option[String]] = IO.pure(None)
      def delete(id: OrderId): IO[Boolean] = IO.pure(false)
      def ping: IO[Boolean] = IO.pure(true)
    }

  private val notReadyStore: OrderStore[IO] =
    new OrderStore[IO] {
      def createAuthorized(
          order: Order,
          items: List[OrderItem]
      ): IO[(Order, List[OrderItem])] =
        IO.raiseError(new NotImplementedError())
      def get(id: OrderId): IO[Option[(Order, List[OrderItem])]] = IO.pure(None)
      def listByCustomer(
          customerId: String
      ): IO[List[(Order, List[OrderItem])]] = IO.pure(Nil)
      def transition(
          id: OrderId,
          event: orderservice.domain.OrderEvent,
          outboxEvent: Option[PendingOutboxEvent] = None,
          reconciliationReason: Option[String] = None
      ): IO[Either[
        orderservice.domain.TransitionRejected,
        UpdatedOrderRef
      ]] =
        IO.pure(Left(orderservice.domain.TransitionRejected.UnknownOrder))
      def flagReconciliation(id: OrderId, reason: String): IO[Boolean] =
        IO.pure(false)
      def reconciliationReason(id: OrderId): IO[Option[String]] = IO.pure(None)
      def delete(id: OrderId): IO[Boolean] = IO.pure(false)
      def ping: IO[Boolean] = IO.pure(false)
    }

  test("GET /health returns 200") {
    val routes = HealthRoutes.routes[IO](readyStore)
    for {
      response <- routes.orNotFound.run(Request[IO](Method.GET, uri"/health"))
    } yield assertEquals(response.status, Status.Ok)
  }

  test("GET /health/ready returns 200 when the store is ready") {
    val routes = HealthRoutes.routes[IO](readyStore)
    for {
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/health/ready")
      )
    } yield assertEquals(response.status, Status.Ok)
  }

  test("GET /health/ready returns 503 when the store is not ready") {
    val routes = HealthRoutes.routes[IO](notReadyStore)
    for {
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/health/ready")
      )
    } yield assertEquals(response.status, Status.ServiceUnavailable)
  }
}
