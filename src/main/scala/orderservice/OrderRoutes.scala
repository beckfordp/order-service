package orderservice

import cats.{Monad, MonadThrow}
import cats.effect.{Async, Clock, Sync}
import cats.syntax.all._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.circe.syntax._
import orderservice.domain.{
  OrderEvent,
  OrderId,
  OrderItemId,
  Pricing,
  TransitionRejected
}
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
    items: List[CreateOrderItemRequest],
    shippingAddress: Address,
    paymentMethodId: String
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
      OrderItemId.render(item.id),
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
    updatedAt: java.time.Instant,
    shippingAddress: Option[Address]
)

object OrderResponse {
  implicit val codec: Codec[OrderResponse] = deriveCodec

  def apply(entity: Order, items: List[OrderItem]): OrderResponse =
    OrderResponse(
      OrderId.render(entity.id),
      entity.customerId,
      entity.totalCents,
      entity.status.asString,
      items.map(OrderItemResponse(_)),
      entity.createdAt,
      entity.updatedAt,
      entity.shippingAddress
    )
}

final case class ErrorResponse(error: String)

object ErrorResponse {
  implicit val codec: Codec[ErrorResponse] = deriveCodec
}

final case class PaymentFailure(reason: String)

object PaymentFailure {
  implicit val codec: Codec[PaymentFailure] = deriveCodec
}

final case class PaymentDeclinedResponse(
    error: String,
    paymentFailure: PaymentFailure
)

object PaymentDeclinedResponse {
  implicit val codec: Codec[PaymentDeclinedResponse] = deriveCodec
}

final case class ReservationFailureResponse(sku: String, reason: String)

object ReservationFailureResponse {
  implicit val codec: Codec[ReservationFailureResponse] = deriveCodec
}

final case class InsufficientStockResponse(
    error: String,
    reservationFailures: List[ReservationFailureResponse]
)

object InsufficientStockResponse {
  implicit val codec: Codec[InsufficientStockResponse] = deriveCodec
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

  private val invalidCheckoutVariant
      : EndpointOutput.OneOfVariant[InvalidCheckout] =
    oneOfVariant(
      statusCode(StatusCode.BadRequest)
        .and(jsonBody[ErrorResponse])
        .map(e => InvalidCheckout(e.error))(e => ErrorResponse(e.message))
    )

  private val illegalTransitionVariant
      : EndpointOutput.OneOfVariant[IllegalTransition] =
    oneOfVariant(
      statusCode(StatusCode.Conflict)
        .and(jsonBody[ErrorResponse])
        .map[IllegalTransition](e => IllegalTransition(e.error))(e =>
          ErrorResponse(s"Illegal status transition to '${e.to}'")
        )
    )

  private val paymentDeclinedVariant
      : EndpointOutput.OneOfVariant[PaymentDeclined] =
    oneOfVariant(
      statusCode(StatusCode.PaymentRequired)
        .and(jsonBody[PaymentDeclinedResponse])
        .map(r => PaymentDeclined(r.paymentFailure.reason))(d =>
          PaymentDeclinedResponse("Payment declined", PaymentFailure(d.reason))
        )
    )

  // `orderItemId` isn't part of the 409 body (gshop matches lines by sku),
  // so decoding - only ever done by tapir clients - can't restore it.
  private val insufficientStockVariant
      : EndpointOutput.OneOfVariant[InsufficientStock] =
    oneOfVariant(
      statusCode(StatusCode.Conflict)
        .and(jsonBody[InsufficientStockResponse])
        .map(r =>
          InsufficientStock(
            r.reservationFailures.map(f => StockFailure("", f.sku, f.reason))
          )
        )(e =>
          InsufficientStockResponse(
            "Insufficient stock",
            e.failures.map(f => ReservationFailureResponse(f.sku, f.reason))
          )
        )
    )

  private def fixedVariant[E <: OrderError: scala.reflect.ClassTag](
      status: StatusCode,
      value: E,
      message: String
  ): EndpointOutput.OneOfVariant[E] =
    oneOfVariant(
      statusCode(status)
        .and(jsonBody[ErrorResponse])
        .map(_ => value)(_ => ErrorResponse(message))
    )

  // Shared by all error-returning endpoints below - most only ever produce a
  // subset of these variants, but reusing one mapping keeps there from being
  // multiple places to update if a shape ever changes. The two 503s share a
  // status, so they're told apart on encode by type and on decode by order.
  private val orderErrorOutput: EndpointOutput[OrderError] =
    oneOf[OrderError](
      notFoundVariant,
      invalidStatusVariant,
      emptyOrderItemsVariant,
      invalidOrderItemVariant,
      invalidCheckoutVariant,
      illegalTransitionVariant,
      paymentDeclinedVariant,
      insufficientStockVariant,
      fixedVariant(
        StatusCode.ServiceUnavailable,
        PaymentServiceUnavailable,
        "Payment service unavailable"
      ),
      fixedVariant(
        StatusCode.ServiceUnavailable,
        InventoryServiceUnavailable,
        "Inventory service unavailable"
      ),
      fixedVariant(
        StatusCode.InternalServerError,
        OrderPersistFailed,
        "Order could not be saved"
      )
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
    OrderId,
    OrderError,
    OrderResponse,
    Any
  ] =
    endpoint.get
      .in("orders" / path[OrderId]("id"))
      .out(jsonBody[OrderResponse])
      .errorOut(orderErrorOutput)

  private val updateOrderEndpoint: PublicEndpoint[
    (OrderId, UpdateOrderRequest),
    OrderError,
    OrderResponse,
    Any
  ] =
    endpoint.patch
      .in("orders" / path[OrderId]("id"))
      .in(jsonBody[UpdateOrderRequest])
      .out(jsonBody[OrderResponse])
      .errorOut(orderErrorOutput)

  private val replaceOrderEndpoint: PublicEndpoint[
    (OrderId, UpdateOrderRequest),
    OrderError,
    OrderResponse,
    Any
  ] =
    endpoint.put
      .in("orders" / path[OrderId]("id"))
      .in(jsonBody[UpdateOrderRequest])
      .out(jsonBody[OrderResponse])
      .errorOut(orderErrorOutput)

  private val deleteOrderEndpoint
      : PublicEndpoint[OrderId, OrderError, Unit, Any] =
    endpoint.delete
      .in("orders" / path[OrderId]("id"))
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

  /** Everything `POST /orders` needs before calling anything downstream: valid
    * items, shipping address and payment method id.
    */
  private def validateCheckout(
      req: CreateOrderRequest
  ): Either[OrderError, List[NewOrderItem]] =
    for {
      items <- validateItems(req.items)
      _ <- Address.validate(req.shippingAddress).leftMap(InvalidCheckout(_))
      _ <- Either
        .catchOnly[IllegalArgumentException](
          java.util.UUID.fromString(req.paymentMethodId)
        )
        .leftMap(_ => InvalidCheckout("paymentMethodId must be a UUID"))
    } yield items

  /** ADR 0018 checkout: validate → mint ids → bulk reserve → authorize →
    * persist as `authorized`. Nothing is written unless every step succeeds;
    * every failure after the reserve call releases whatever it holds (also when
    * the reserve call itself raised, since it may have landed). A failed
    * release is logged and otherwise ignored — it's idempotent, and a missed
    * one is TD-8's orphaned reservation.
    */
  def serverEndpoint[F[_]: Sync: Clock](
      store: OrderStore[F],
      logger: StructuredLogger[F],
      inventoryClient: InventoryClient[F],
      paymentClient: PaymentClient[F]
  ): ServerEndpoint[Any, F] =
    createOrderEndpoint.serverLogic[F] { req =>
      def release(orderId: OrderId): F[Unit] = {
        val renderedId = OrderId.render(orderId)
        inventoryClient.release(renderedId).handleErrorWith { error =>
          logger.warn(Map("order_id" -> renderedId), error)(
            "Releasing the reservation failed"
          )
        }
      }

      def fail(orderId: OrderId, error: OrderError, releaseFirst: Boolean)(
          log: F[Unit]
      ): F[Either[OrderError, OrderResponse]] =
        log *> release(orderId).whenA(releaseFirst).as(Left(error))

      def place(
          items: List[NewOrderItem]
      ): F[Either[OrderError, OrderResponse]] = {
        val lines =
          items.map(i => Pricing.Line(i.unitPriceCents, i.quantity))
        Pricing.totalCents(lines) match {
          case Left(error)       => Sync[F].pure(Left(InvalidCheckout(error)))
          case Right(totalCents) => placeWithTotal(items, totalCents)
        }
      }

      def placeWithTotal(
          items: List[NewOrderItem],
          totalCents: Int
      ): F[Either[OrderError, OrderResponse]] =
        for {
          orderId <- Sync[F].delay(
            OrderId.fromUuid(java.util.UUID.randomUUID())
          )
          now <- Clock[F].realTimeInstant
          orderItems <- items.traverse { item =>
            Sync[F]
              .delay(OrderItemId.fromUuid(java.util.UUID.randomUUID()))
              .map { id =>
                OrderItem(
                  id,
                  orderId,
                  item.sku,
                  item.productName,
                  item.unitPriceCents,
                  item.quantity
                )
              }
          }
          order = Order(
            orderId,
            req.customerId,
            totalCents,
            OrderStatus.Authorized,
            now,
            now,
            Some(req.shippingAddress)
          )
          renderedOrderId = OrderId.render(orderId)
          context = Map(
            "order_id" -> renderedOrderId,
            "customer_id" -> req.customerId
          )
          reserved <- inventoryClient
            .reserveOrder(
              renderedOrderId,
              orderItems.map(i =>
                OrderReservationLine(
                  OrderItemId.render(i.id),
                  i.sku,
                  i.quantity
                )
              )
            )
            .attempt
          result <- reserved match {
            case Left(error) =>
              fail(orderId, InventoryServiceUnavailable, releaseFirst = true)(
                logger.warn(context, error)("Reservation call failed")
              )
            case Right(OrderReservationResult.Rejected(failures)) =>
              fail(orderId, InsufficientStock(failures), releaseFirst = false)(
                logger.warn(
                  context + ("failed_lines" -> failures.size.toString)
                )(
                  "Reservation rejected"
                )
              )
            case Right(OrderReservationResult.Reserved) =>
              authorizeAndPersist(order, orderItems, context)
          }
        } yield result

      def authorizeAndPersist(
          order: Order,
          items: List[OrderItem],
          context: Map[String, String]
      ): F[Either[OrderError, OrderResponse]] =
        paymentClient
          .authorize(
            OrderId.render(order.id),
            order.totalCents,
            req.paymentMethodId
          )
          .attempt
          .flatMap {
            case Left(error) =>
              fail(order.id, PaymentServiceUnavailable, releaseFirst = true)(
                logger.warn(context, error)("Authorize call failed")
              )
            case Right(AuthorizationResult.Declined(reason)) =>
              fail(order.id, PaymentDeclined(reason), releaseFirst = true)(
                logger.warn(context + ("reason" -> reason))("Payment declined")
              )
            case Right(AuthorizationResult.UnknownPaymentMethod) =>
              fail(
                order.id,
                InvalidCheckout("Payment method not found"),
                releaseFirst = true
              )(logger.warn(context)("Unknown payment method"))
            case Right(AuthorizationResult.Authorized(_, _)) =>
              store.createAuthorized(order, items).attempt.flatMap {
                case Left(error) =>
                  // The authorization is left to lapse via Journey 5's expiry
                  // sweep: payment-service has no void endpoint yet.
                  fail(order.id, OrderPersistFailed, releaseFirst = true)(
                    logger.error(context, error)("Persisting the order failed")
                  )
                case Right((persisted, persistedItems)) =>
                  logger
                    .info(context)("Request completed")
                    .as(Right(OrderResponse(persisted, persistedItems)))
              }
          }

      for {
        _ <- logger.info(
          Map(
            "method" -> "POST",
            "path" -> "/orders"
          )
        )("Received request")
        result <- validateCheckout(req) match {
          case Left(error) =>
            logger
              .warn(
                Map(
                  "customer_id" -> req.customerId,
                  "reason" -> error.toString
                )
              )("Invalid checkout request")
              .as(Left(error): Either[OrderError, OrderResponse])
          case Right(items) => place(items)
        }
      } yield result
    }

  /** Cache-aside: a hit returns the cached history as-is; a miss queries the
    * store, populates the cache (TTL-only freshness - see spec.md's "Out of
    * Scope"), and returns the freshly-queried result.
    */
  def listOrdersServerEndpoint[F[_]: MonadThrow](
      store: OrderStore[F],
      logger: StructuredLogger[F],
      historyCache: OrderHistoryCache[F]
  ): ServerEndpoint[Any, F] =
    def fromStore(customerId: String): F[List[OrderResponse]] =
      for {
        orders <- store.listByCustomer(customerId)
        history = orders.map { case (order, items) =>
          OrderResponse(order, items)
        }
        _ <- historyCache
          .set(customerId, history)
          .attempt
          .flatMap {
            case Left(error) =>
              logger.warn(
                Map("customer_id" -> customerId, "cache" -> "write_error"),
                error
              )("Order-history cache write failed - response unaffected")
            case Right(_) => Monad[F].unit
          }
        _ <- logger.info(
          Map("customer_id" -> customerId, "cache" -> "miss")
        )("Request completed")
      } yield history
    listOrdersEndpoint.serverLogicSuccess[F] { customerId =>
      for {
        _ <- logger.info(
          Map(
            "method" -> "GET",
            "path" -> "/orders",
            "customer_id" -> customerId
          )
        )("Received request")
        cached <- historyCache.get(customerId).attempt
        result <- cached match {
          case Right(Some(history)) =>
            logger
              .info(Map("customer_id" -> customerId, "cache" -> "hit"))(
                "Request completed"
              )
              .as(history)
          case Right(None) => fromStore(customerId)
          case Left(error) =>
            logger.warn(
              Map("customer_id" -> customerId, "cache" -> "read_error"),
              error
            )("Order-history cache read failed - falling back to Postgres") *>
              fromStore(customerId)
        }
      } yield result
    }

  def getOrderServerEndpoint[F[_]: Monad](
      store: OrderStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    getOrderEndpoint.serverLogic[F] { id =>
      val renderedId = OrderId.render(id)
      for {
        _ <- logger.info(
          Map(
            "method" -> "GET",
            "path" -> s"/orders/$renderedId",
            "order_id" -> renderedId
          )
        )(
          "Received request"
        )
        result <- store.get(id).flatMap {
          case Some((order, items)) =>
            logger
              .info(Map("order_id" -> renderedId))("Request completed")
              .as(Right(OrderResponse(order, items)))
          case None =>
            logger
              .warn(Map("order_id" -> renderedId))("Order not found")
              .as(Left(OrderNotFound))
        }
      } yield result
    }

  /** Shared handler for `PATCH` (partial update) and `PUT` (full replace) —
    * both call `OrderStore.update` with the same required update body; only the
    * logged HTTP method differs.
    */
  private def updateLogic[F[_]: Monad: Clock](
      store: OrderStore[F],
      logger: StructuredLogger[F],
      httpMethod: String
  )(
      id: OrderId,
      req: UpdateOrderRequest
  ): F[Either[OrderError, OrderResponse]] = {
    val renderedId = OrderId.render(id)

    def respond(
        result: Either[TransitionRejected, UpdatedOrderRef]
    ): F[Either[OrderError, OrderResponse]] =
      result match {
        case Right(_) =>
          store.get(id).flatMap {
            case Some((order, items)) =>
              logger
                .info(Map("order_id" -> renderedId))("Request completed")
                .as(Right(OrderResponse(order, items)))
            case None =>
              logger
                .warn(Map("order_id" -> renderedId))("Order not found")
                .as(Left(OrderNotFound))
          }
        case Left(TransitionRejected.UnknownOrder) =>
          logger
            .warn(Map("order_id" -> renderedId))("Order not found")
            .as(Left(OrderNotFound))
        case Left(TransitionRejected.IllegalTransition(from, to)) =>
          logger
            .warn(
              Map(
                "order_id" -> renderedId,
                "from" -> from.asString,
                "to" -> to.asString
              )
            )("Illegal status transition")
            .as(Left(IllegalTransition(to.asString)))
      }

    for {
      _ <- logger.info(
        Map(
          "method" -> httpMethod,
          "path" -> s"/orders/$renderedId",
          "order_id" -> renderedId
        )
      )("Received request")
      result <- OrderStatus.fromString(req.status) match {
        case Left(_) =>
          logger
            .warn(Map("order_id" -> renderedId, "status" -> req.status))(
              "Invalid status"
            )
            .as(Left(InvalidStatus(req.status)))
        case Right(OrderStatus.Cancelled) =>
          Clock[F].realTimeInstant.flatMap { now =>
            store
              .transition(
                id,
                OrderEvent.CustomerCancelled,
                Some(
                  PendingOutboxEvent(
                    "OrderCancelled",
                    ref =>
                      OrderCancelledEvent(
                        OrderId.render(ref.orderId),
                        ref.customerId,
                        now
                      ).asJson
                  )
                )
              )
              .flatMap(respond)
          }
        case Right(other) =>
          logger
            .warn(Map("order_id" -> renderedId, "status" -> other.asString))(
              "Status is not reachable through PATCH/PUT"
            )
            .as(Left(IllegalTransition(other.asString)))
      }
    } yield result
  }

  def updateOrderServerEndpoint[F[_]: Monad: Clock](
      store: OrderStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    updateOrderEndpoint.serverLogic[F] { case (id, req) =>
      updateLogic(store, logger, "PATCH")(id, req)
    }

  def replaceOrderServerEndpoint[F[_]: Monad: Clock](
      store: OrderStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    replaceOrderEndpoint.serverLogic[F] { case (id, req) =>
      updateLogic(store, logger, "PUT")(id, req)
    }

  def deleteOrderServerEndpoint[F[_]: Monad](
      store: OrderStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    deleteOrderEndpoint.serverLogic[F] { id =>
      val renderedId = OrderId.render(id)
      for {
        _ <- logger.info(
          Map(
            "method" -> "DELETE",
            "path" -> s"/orders/$renderedId",
            "order_id" -> renderedId
          )
        )("Received request")
        result <- store.delete(id).flatMap {
          case true =>
            logger
              .info(Map("order_id" -> renderedId))("Request completed")
              .as(Right(()))
          case false =>
            logger
              .warn(Map("order_id" -> renderedId))("Order not found")
              .as(Left(OrderNotFound))
        }
      } yield result
    }

  def routes[F[_]: Async](
      store: OrderStore[F],
      logger: StructuredLogger[F],
      inventoryClient: InventoryClient[F],
      paymentClient: PaymentClient[F],
      historyCache: OrderHistoryCache[F]
  ): HttpRoutes[F] =
    Http4sServerInterpreter[F]().toRoutes(
      List(
        serverEndpoint(store, logger, inventoryClient, paymentClient),
        listOrdersServerEndpoint(store, logger, historyCache),
        getOrderServerEndpoint(store, logger),
        updateOrderServerEndpoint(store, logger),
        replaceOrderServerEndpoint(store, logger),
        deleteOrderServerEndpoint(store, logger)
      )
    )
}
