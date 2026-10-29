package orderservice

import cats.{Applicative, Monad}
import cats.effect.Async
import cats.syntax.all._
import org.http4s.HttpRoutes
import sttp.model.StatusCode
import sttp.tapir._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.http4s.Http4sServerInterpreter

object HealthRoutes {

  private val healthEndpoint: PublicEndpoint[Unit, Unit, Unit, Any] =
    endpoint.get.in("health").out(statusCode(StatusCode.Ok))

  private val readyEndpoint: PublicEndpoint[Unit, Unit, Unit, Any] =
    endpoint.get
      .in("health" / "ready")
      .out(statusCode(StatusCode.Ok))
      .errorOut(statusCode(StatusCode.ServiceUnavailable))

  def healthServerEndpoint[F[_]: Applicative]: ServerEndpoint[Any, F] =
    healthEndpoint.serverLogicSuccess[F](_ => Applicative[F].unit)

  def readyServerEndpoint[F[_]: Monad](
      store: OrderStore[F]
  ): ServerEndpoint[Any, F] =
    readyEndpoint.serverLogic[F] { _ =>
      store.ping.map(ready => if (ready) Right(()) else Left(()))
    }

  def routes[F[_]: Async](store: OrderStore[F]): HttpRoutes[F] =
    Http4sServerInterpreter[F]().toRoutes(
      List(healthServerEndpoint[F], readyServerEndpoint(store))
    )
}
