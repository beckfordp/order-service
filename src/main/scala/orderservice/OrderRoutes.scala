package orderservice

import cats.effect.Async
import cats.syntax.all._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.http4s.HttpRoutes
import org.typelevel.log4cats.StructuredLogger
import sttp.model.StatusCode
import sttp.tapir._
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.http4s.Http4sServerInterpreter

final case class CreateOrderRequest(customerId: String, totalCents: Int)

object CreateOrderRequest {
  implicit val codec: Codec[CreateOrderRequest] = deriveCodec
}

final case class UpdateOrderRequest(status: String)

object UpdateOrderRequest {
  implicit val codec: Codec[UpdateOrderRequest] = deriveCodec
}

final case class OrderResponse(
    id: String,
    customerId: String,
    totalCents: Int,
    status: String,
    createdAt: java.time.Instant,
    updatedAt: java.time.Instant
)

object OrderResponse {
  implicit val codec: Codec[OrderResponse] = deriveCodec

  def apply(entity: Order): OrderResponse =
    OrderResponse(
      entity.id,
      entity.customerId,
      entity.totalCents,
      entity.status.asString,
      entity.createdAt,
      entity.updatedAt
    )
}

final case class ErrorResponse(error: String)

object ErrorResponse {
  implicit val codec: Codec[ErrorResponse] = deriveCodec
}

object OrderRoutes {

  private val createOrderEndpoint: PublicEndpoint[
    CreateOrderRequest,
    Unit,
    OrderResponse,
    Any
  ] =
    endpoint.post
      .in("orders")
      .in(jsonBody[CreateOrderRequest])
      .out(statusCode(StatusCode.Created))
      .out(jsonBody[OrderResponse])

  private val notFoundOutput: EndpointOutput[OrderError] =
    statusCode(StatusCode.NotFound)
      .and(jsonBody[ErrorResponse])
      .map[OrderError](_ => OrderNotFound)(_ =>
        ErrorResponse("Order not found")
      )

  private val notFoundVariant: EndpointOutput.OneOfVariant[OrderNotFound.type] =
    oneOfVariant(
      statusCode(StatusCode.NotFound)
        .and(jsonBody[ErrorResponse])
        .map[OrderNotFound.type](_ => OrderNotFound)(_ =>
          ErrorResponse("Order not found")
        )
    )

  private val invalidStatusVariant: EndpointOutput.OneOfVariant[InvalidStatus] =
    oneOfVariant(
      statusCode(StatusCode.BadRequest)
        .and(jsonBody[ErrorResponse])
        .map[InvalidStatus](e => InvalidStatus(e.error))(e =>
          ErrorResponse(s"Invalid order status: '${e.raw}'")
        )
    )

  private val updateErrorOutput: EndpointOutput[OrderError] =
    oneOf[OrderError](notFoundVariant, invalidStatusVariant)

  private val getOrderEndpoint: PublicEndpoint[
    String,
    OrderError,
    OrderResponse,
    Any
  ] =
    endpoint.get
      .in("orders" / path[String]("id"))
      .out(jsonBody[OrderResponse])
      .errorOut(notFoundOutput)

  private val updateOrderEndpoint: PublicEndpoint[
    (String, UpdateOrderRequest),
    OrderError,
    OrderResponse,
    Any
  ] =
    endpoint.patch
      .in("orders" / path[String]("id"))
      .in(jsonBody[UpdateOrderRequest])
      .out(jsonBody[OrderResponse])
      .errorOut(updateErrorOutput)

  private val replaceOrderEndpoint: PublicEndpoint[
    (String, UpdateOrderRequest),
    OrderError,
    OrderResponse,
    Any
  ] =
    endpoint.put
      .in("orders" / path[String]("id"))
      .in(jsonBody[UpdateOrderRequest])
      .out(jsonBody[OrderResponse])
      .errorOut(updateErrorOutput)

  private val deleteOrderEndpoint
      : PublicEndpoint[String, OrderError, Unit, Any] =
    endpoint.delete
      .in("orders" / path[String]("id"))
      .out(statusCode(StatusCode.NoContent))
      .errorOut(notFoundOutput)

  def serverEndpoint[F[_]: Async](
      store: OrderStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    createOrderEndpoint.serverLogicSuccess[F] { req =>
      for {
        _ <- logger.info(
          Map(
            "method" -> "POST",
            "path" -> "/orders"
          )
        )("Received request")
        entity <- store.create(req.customerId, req.totalCents).onError {
          case error =>
            logger.error(Map.empty, error)("Persisting the order failed")
        }
        _ <- logger.info(
          Map("order_id" -> entity.id)
        )("Request completed")
      } yield OrderResponse(entity)
    }

  def getOrderServerEndpoint[F[_]: Async](
      store: OrderStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    getOrderEndpoint.serverLogic[F] { id =>
      for {
        _ <- logger.info(
          Map("method" -> "GET", "path" -> s"/orders/$id", "order_id" -> id)
        )(
          "Received request"
        )
        result <- store.get(id).flatMap {
          case Some(entity) =>
            logger
              .info(Map("order_id" -> id))("Request completed")
              .as(Right(OrderResponse(entity)))
          case None =>
            logger
              .warn(Map("order_id" -> id))("Order not found")
              .as(Left(OrderNotFound))
        }
      } yield result
    }

  /** Shared handler for `PATCH` (partial update) and `PUT` (full replace) —
    * both call `OrderStore.update` with the same required update body; only the
    * logged HTTP method differs.
    */
  private def updateLogic[F[_]: Async](
      store: OrderStore[F],
      logger: StructuredLogger[F],
      httpMethod: String
  )(
      id: String,
      req: UpdateOrderRequest
  ): F[Either[OrderError, OrderResponse]] =
    for {
      _ <- logger.info(
        Map(
          "method" -> httpMethod,
          "path" -> s"/orders/$id",
          "order_id" -> id
        )
      )("Received request")
      result <- OrderStatus.fromString(req.status) match {
        case Left(_) =>
          logger
            .warn(Map("order_id" -> id, "status" -> req.status))(
              "Invalid status"
            )
            .as(Left(InvalidStatus(req.status)))
        case Right(status) =>
          store.update(id, status).flatMap {
            case Some(entity) =>
              logger
                .info(Map("order_id" -> id))("Request completed")
                .as(Right(OrderResponse(entity)))
            case None =>
              logger
                .warn(Map("order_id" -> id))("Order not found")
                .as(Left(OrderNotFound))
          }
      }
    } yield result

  def updateOrderServerEndpoint[F[_]: Async](
      store: OrderStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    updateOrderEndpoint.serverLogic[F] { case (id, req) =>
      updateLogic(store, logger, "PATCH")(id, req)
    }

  def replaceOrderServerEndpoint[F[_]: Async](
      store: OrderStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    replaceOrderEndpoint.serverLogic[F] { case (id, req) =>
      updateLogic(store, logger, "PUT")(id, req)
    }

  def deleteOrderServerEndpoint[F[_]: Async](
      store: OrderStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    deleteOrderEndpoint.serverLogic[F] { id =>
      for {
        _ <- logger.info(
          Map("method" -> "DELETE", "path" -> s"/orders/$id", "order_id" -> id)
        )("Received request")
        result <- store.delete(id).flatMap {
          case true =>
            logger
              .info(Map("order_id" -> id))("Request completed")
              .as(Right(()))
          case false =>
            logger
              .warn(Map("order_id" -> id))("Order not found")
              .as(Left(OrderNotFound))
        }
      } yield result
    }

  def routes[F[_]: Async](
      store: OrderStore[F],
      logger: StructuredLogger[F]
  ): HttpRoutes[F] =
    Http4sServerInterpreter[F]().toRoutes(
      List(
        serverEndpoint(store, logger),
        getOrderServerEndpoint(store, logger),
        updateOrderServerEndpoint(store, logger),
        replaceOrderServerEndpoint(store, logger),
        deleteOrderServerEndpoint(store, logger)
      )
    )
}
