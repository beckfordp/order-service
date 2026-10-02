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

final case class CreateOrderItemRequest(
    sku: String,
    productName: String,
    unitPriceCents: Int,
    quantity: Int
)

object CreateOrderItemRequest {
  implicit val codec: Codec[CreateOrderItemRequest] = deriveCodec
}

final case class CreateOrderRequest(
    customerId: String,
    items: List[CreateOrderItemRequest]
)

object CreateOrderRequest {
  implicit val codec: Codec[CreateOrderRequest] = deriveCodec
}

final case class UpdateOrderRequest(status: String)

object UpdateOrderRequest {
  implicit val codec: Codec[UpdateOrderRequest] = deriveCodec
}

final case class OrderItemResponse(
    id: String,
    sku: String,
    productName: String,
    unitPriceCents: Int,
    quantity: Int
)

object OrderItemResponse {
  implicit val codec: Codec[OrderItemResponse] = deriveCodec

  def apply(item: OrderItem): OrderItemResponse =
    OrderItemResponse(
      item.id,
      item.sku,
      item.productName,
      item.unitPriceCents,
      item.quantity
    )
}

final case class OrderResponse(
    id: String,
    customerId: String,
    totalCents: Int,
    status: String,
    items: List[OrderItemResponse],
    createdAt: java.time.Instant,
    updatedAt: java.time.Instant
)

object OrderResponse {
  implicit val codec: Codec[OrderResponse] = deriveCodec

  def apply(entity: Order, items: List[OrderItem]): OrderResponse =
    OrderResponse(
      entity.id,
      entity.customerId,
      entity.totalCents,
      entity.status.asString,
      items.map(OrderItemResponse(_)),
      entity.createdAt,
      entity.updatedAt
    )
}

final case class ErrorResponse(error: String)

object ErrorResponse {
  implicit val codec: Codec[ErrorResponse] = deriveCodec
}

object OrderRoutes {

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

  private val emptyOrderItemsVariant
      : EndpointOutput.OneOfVariant[EmptyOrderItems.type] =
    oneOfVariant(
      statusCode(StatusCode.BadRequest)
        .and(jsonBody[ErrorResponse])
        .map[EmptyOrderItems.type](_ => EmptyOrderItems)(_ =>
          ErrorResponse("Order must have at least one item")
        )
    )

  private val invalidOrderItemVariant
      : EndpointOutput.OneOfVariant[InvalidOrderItem] =
    oneOfVariant(
      statusCode(StatusCode.BadRequest)
        .and(jsonBody[ErrorResponse])
        .map[InvalidOrderItem](e => InvalidOrderItem(e.error))(e =>
          ErrorResponse(e.reason)
        )
    )

  // Shared by all five error-returning endpoints below - most only ever
  // produce a subset of these variants, but reusing one mapping keeps there
  // from being multiple places to update if a shape ever changes.
  private val orderErrorOutput: EndpointOutput[OrderError] =
    oneOf[OrderError](
      notFoundVariant,
      invalidStatusVariant,
      emptyOrderItemsVariant,
      invalidOrderItemVariant
    )

  private val createOrderEndpoint: PublicEndpoint[
    CreateOrderRequest,
    OrderError,
    OrderResponse,
    Any
  ] =
    endpoint.post
      .in("orders")
      .in(jsonBody[CreateOrderRequest])
      .out(statusCode(StatusCode.Created))
      .out(jsonBody[OrderResponse])
      .errorOut(orderErrorOutput)

  private val listOrdersEndpoint: PublicEndpoint[
    String,
    Unit,
    List[OrderResponse],
    Any
  ] =
    endpoint.get
      .in("orders")
      .in(query[String]("customerId"))
      .out(jsonBody[List[OrderResponse]])

  private val getOrderEndpoint: PublicEndpoint[
    String,
    OrderError,
    OrderResponse,
    Any
  ] =
    endpoint.get
      .in("orders" / path[String]("id"))
      .out(jsonBody[OrderResponse])
      .errorOut(orderErrorOutput)

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
      .errorOut(orderErrorOutput)

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
      .errorOut(orderErrorOutput)

  private val deleteOrderEndpoint
      : PublicEndpoint[String, OrderError, Unit, Any] =
    endpoint.delete
      .in("orders" / path[String]("id"))
      .out(statusCode(StatusCode.NoContent))
      .errorOut(orderErrorOutput)

  /** Validates checkout's line items before anything is persisted: at least one
    * item is required, and each item's quantity/price must be sane.
    * Short-circuits on the first violation (not a full accumulation of every
    * bad item) - good enough for a 400 the client has to fix anyway.
    */
  private def validateItems(
      items: List[CreateOrderItemRequest]
  ): Either[OrderError, List[NewOrderItem]] =
    if (items.isEmpty) Left(EmptyOrderItems)
    else
      items.traverse { item =>
        if (item.quantity <= 0)
          Left(
            InvalidOrderItem(
              s"quantity must be positive, got ${item.quantity} for sku '${item.sku}'"
            )
          )
        else if (item.unitPriceCents < 0)
          Left(
            InvalidOrderItem(
              s"unitPriceCents must be non-negative, got ${item.unitPriceCents} for sku '${item.sku}'"
            )
          )
        else
          Right(
            NewOrderItem(
              item.sku,
              item.productName,
              item.unitPriceCents,
              item.quantity
            )
          )
      }

  /** Attempts to reserve every item in order, sequentially, short-circuiting at
    * the first item that can't be reserved (all-or-nothing - see US-4.2's spec,
    * "Out of Scope" re: partial fulfillment). Returns `Left(reason)` for the
    * first failure - a non-`Reserved` result or the client itself raising (e.g.
    * the circuit breaker open, retries exhausted) - never calling `reserve` for
    * any item after that point.
    */
  private def reserveAll[F[_]: Async](
      inventoryClient: InventoryClient[F],
      items: List[OrderItem]
  ): F[Either[String, Unit]] =
    items.foldLeft(Async[F].pure(Right(()): Either[String, Unit])) {
      (acc, item) =>
        acc.flatMap {
          case failed @ Left(_) => Async[F].pure(failed)
          case Right(())        =>
            inventoryClient
              .reserve(item.sku, item.quantity, item.id)
              .attempt
              .map {
                case Right(ReservationResult.Reserved)          => Right(())
                case Right(ReservationResult.InsufficientStock) =>
                  Left(s"insufficient stock for sku '${item.sku}'")
                case Right(ReservationResult.UnknownSku) =>
                  Left(s"unknown sku '${item.sku}'")
                case Left(error) =>
                  Left(
                    s"reservation call failed for sku '${item.sku}': ${error.getMessage}"
                  )
              }
        }
    }

  def serverEndpoint[F[_]: Async](
      store: OrderStore[F],
      logger: StructuredLogger[F],
      inventoryClient: InventoryClient[F],
      publisher: OrderEventPublisher[F]
  ): ServerEndpoint[Any, F] =
    createOrderEndpoint.serverLogic[F] { req =>
      for {
        _ <- logger.info(
          Map(
            "method" -> "POST",
            "path" -> "/orders"
          )
        )("Received request")
        result <- validateItems(req.items) match {
          case Left(error) =>
            logger
              .warn(
                Map(
                  "customer_id" -> req.customerId,
                  "reason" -> error.toString
                )
              )("Invalid order items")
              .as(Left(error): Either[OrderError, OrderResponse])
          case Right(items) =>
            for {
              created <- store.create(req.customerId, items).onError {
                case error =>
                  logger.error(Map.empty, error)("Persisting the order failed")
              }
              (order, orderItems) = created
              reservation <- reserveAll(inventoryClient, orderItems)
              finalOrder <- reservation match {
                case Right(()) =>
                  logger
                    .info(Map("order_id" -> order.id))("Request completed")
                    .as((order, orderItems))
                case Left(reason) =>
                  for {
                    _ <- logger.warn(
                      Map("order_id" -> order.id, "reason" -> reason)
                    )("Reservation failed")
                    // Known edge case (review finding, US-4.2): if `update` returns
                    // None - e.g. the order row vanishes between creation and this
                    // call - the response falls back to the stale `order` object,
                    // which still reports `pending` instead of `reservation_failed`.
                    // Accepted risk: requires the just-created order to disappear
                    // within the same request. Not fixed; tracked in the backlog.
                    updated <- store.update(
                      order.id,
                      OrderStatus.ReservationFailed
                    )
                    _ <- updated match {
                      case Some((updatedOrder, _)) =>
                        Async[F].realTimeInstant.flatMap { now =>
                          publisher.publishStatusChanged(
                            OrderStatusChangedEvent(
                              updatedOrder.id,
                              updatedOrder.customerId,
                              "reservation_failed",
                              now
                            )
                          )
                        }
                      case None => Async[F].unit
                    }
                  } yield updated.getOrElse((order, orderItems))
              }
            } yield Right(
              OrderResponse(finalOrder._1, finalOrder._2)
            ): Either[OrderError, OrderResponse]
        }
      } yield result
    }

  /** Cache-aside: a hit returns the cached history as-is; a miss queries the
    * store, populates the cache (TTL-only freshness - see spec.md's "Out of
    * Scope"), and returns the freshly-queried result.
    */
  def listOrdersServerEndpoint[F[_]: Async](
      store: OrderStore[F],
      logger: StructuredLogger[F],
      historyCache: OrderHistoryCache[F]
  ): ServerEndpoint[Any, F] =
    listOrdersEndpoint.serverLogicSuccess[F] { customerId =>
      for {
        _ <- logger.info(
          Map(
            "method" -> "GET",
            "path" -> "/orders",
            "customer_id" -> customerId
          )
        )("Received request")
        cached <- historyCache.get(customerId)
        result <- cached match {
          case Some(history) =>
            logger
              .info(Map("customer_id" -> customerId, "cache" -> "hit"))(
                "Request completed"
              )
              .as(history)
          case None =>
            for {
              orders <- store.listByCustomer(customerId)
              history = orders.map { case (order, items) =>
                OrderResponse(order, items)
              }
              _ <- historyCache.set(customerId, history)
              _ <- logger.info(
                Map("customer_id" -> customerId, "cache" -> "miss")
              )("Request completed")
            } yield history
        }
      } yield result
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
          case Some((order, items)) =>
            logger
              .info(Map("order_id" -> id))("Request completed")
              .as(Right(OrderResponse(order, items)))
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
            case Some((order, items)) =>
              logger
                .info(Map("order_id" -> id))("Request completed")
                .as(Right(OrderResponse(order, items)))
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
      logger: StructuredLogger[F],
      inventoryClient: InventoryClient[F],
      historyCache: OrderHistoryCache[F],
      publisher: OrderEventPublisher[F]
  ): HttpRoutes[F] =
    Http4sServerInterpreter[F]().toRoutes(
      List(
        serverEndpoint(store, logger, inventoryClient, publisher),
        listOrdersServerEndpoint(store, logger, historyCache),
        getOrderServerEndpoint(store, logger),
        updateOrderServerEndpoint(store, logger),
        replaceOrderServerEndpoint(store, logger),
        deleteOrderServerEndpoint(store, logger)
      )
    )
}
