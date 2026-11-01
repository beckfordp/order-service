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
import sttp.tapir.Schema.annotations.{description, encodedExample}
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.http4s.Http4sServerInterpreter

@description("One line to buy.")
final case class CreateOrderItemRequest(
    @description("Catalog SKU.") @encodedExample("SKU-MUG-001")
    sku: String,
    @description("Name shown on the order.") @encodedExample("Gluon Mug")
    productName: String,
    @description("Unit price in cents, at least 0.") @encodedExample(1299)
    unitPriceCents: Int,
    @description("Units to buy, at least 1.") @encodedExample(2)
    quantity: Int
)

object CreateOrderItemRequest {
  implicit val codec: Codec[CreateOrderItemRequest] = deriveCodec
}

@description("A checkout: what to buy, where to ship it and how to pay.")
final case class CreateOrderRequest(
    @description("Id (UUID) of the customer placing the order.")
    @encodedExample("7d1e4b2a-9c3f-4e8a-b5d6-1f2a3b4c5d6e")
    customerId: String,
    @description("Lines to buy; at least one.")
    items: List[CreateOrderItemRequest],
    @description("Where to ship the order.")
    shippingAddress: Address,
    @description("Saved payment method (UUID) to authorize against.")
    @encodedExample("9b2e4c7a-1f3d-4a6b-8c5e-2d7f0a1b3c4e")
    paymentMethodId: String
)

object CreateOrderRequest {
  implicit val codec: Codec[CreateOrderRequest] = deriveCodec
}

@description("A status change.")
final case class UpdateOrderRequest(
    @description("Target status; only `cancelled` is accepted.")
    @encodedExample("cancelled")
    status: String
)

object UpdateOrderRequest {
  implicit val codec: Codec[UpdateOrderRequest] = deriveCodec
}

@description("One line of an order.")
final case class OrderItemResponse(
    @description("Line id (UUID).")
    @encodedExample("5c8d2e1f-3a4b-4c6d-9e7f-0a1b2c3d4e5f")
    id: String,
    @description("Catalog SKU.") @encodedExample("SKU-MUG-001")
    sku: String,
    @description("Name shown on the order.") @encodedExample("Gluon Mug")
    productName: String,
    @description("Unit price in cents.") @encodedExample(1299)
    unitPriceCents: Int,
    @description("Units bought.") @encodedExample(2)
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

@description("An order with its line items.")
final case class OrderResponse(
    @description("Order id (UUID).")
    @encodedExample("3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90")
    id: String,
    @description("Id (UUID) of the customer who placed the order.")
    @encodedExample("7d1e4b2a-9c3f-4e8a-b5d6-1f2a3b4c5d6e")
    customerId: String,
    @description("Sum of the line totals, in cents.") @encodedExample(2598)
    totalCents: Int,
    @description("Lifecycle status.") @encodedExample("authorized")
    status: String,
    @description("The order's lines, by SKU.")
    items: List[OrderItemResponse],
    @description("When the order was placed (UTC).")
    @encodedExample("2026-11-01T09:30:00Z")
    createdAt: java.time.Instant,
    @description("When the order last changed (UTC).")
    @encodedExample("2026-11-01T09:45:00Z")
    updatedAt: java.time.Instant,
    @description("Where to ship; absent on orders placed before US-11.1.")
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

@description("Error body.")
final case class ErrorResponse(
    @description("Human-readable reason.") @encodedExample("Order not found")
    error: String
)

object ErrorResponse {
  implicit val codec: Codec[ErrorResponse] = deriveCodec
}

@description("The provider's decline detail.")
final case class PaymentFailure(
    @description("Why the payment provider declined.")
    @encodedExample("insufficient_funds")
    reason: String
)

object PaymentFailure {
  implicit val codec: Codec[PaymentFailure] = deriveCodec
}

@description("Error body for a declined payment.")
final case class PaymentDeclinedResponse(
    @description("Human-readable reason.") @encodedExample("Payment declined")
    error: String,
    paymentFailure: PaymentFailure
)

object PaymentDeclinedResponse {
  implicit val codec: Codec[PaymentDeclinedResponse] = deriveCodec
}

@description("A line that couldn't be reserved.")
final case class ReservationFailureResponse(
    @description("SKU of the line.") @encodedExample("SKU-MUG-001")
    sku: String,
    @description("Why it couldn't be reserved.") @encodedExample("out_of_stock")
    reason: String
)

object ReservationFailureResponse {
  implicit val codec: Codec[ReservationFailureResponse] = deriveCodec
}

@description("Error body for lines that couldn't be reserved.")
final case class InsufficientStockResponse(
    @description("Human-readable reason.")
    @encodedExample("Insufficient stock")
    error: String,
    @description("Every line that couldn't be reserved.")
    reservationFailures: List[ReservationFailureResponse]
)

object InsufficientStockResponse {
  implicit val codec: Codec[InsufficientStockResponse] = deriveCodec
}

object OrderRoutes {

  // REST docs convention (purerest README "Documenting endpoints"): every
  // endpoint, parameter, DTO field and error is described, and OrderDocsSuite
  // asserts Docs.undocumented finds no gaps.
  private val tag = "orders"

  // Each fixed error's body, shared by its documented example and its real
  // mapping.
  private val notFound = ErrorResponse("Order not found")
  private val emptyItems = ErrorResponse("Order must have at least one item")

  private val exampleAddress =
    Address("1 Example Street", Some("Flat 2"), "London", "EC1A 1AA", "GB")

  private val exampleItem =
    CreateOrderItemRequest("SKU-MUG-001", "Gluon Mug", 1299, 2)

  private val exampleOrder = OrderResponse(
    "3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90",
    "7d1e4b2a-9c3f-4e8a-b5d6-1f2a3b4c5d6e",
    2598,
    "authorized",
    List(
      OrderItemResponse(
        "5c8d2e1f-3a4b-4c6d-9e7f-0a1b2c3d4e5f",
        exampleItem.sku,
        exampleItem.productName,
        exampleItem.unitPriceCents,
        exampleItem.quantity
      )
    ),
    java.time.Instant.parse("2026-11-01T09:30:00Z"),
    java.time.Instant.parse("2026-11-01T09:45:00Z"),
    Some(exampleAddress)
  )

  private val idPath: EndpointInput.PathCapture[OrderId] =
    path[OrderId]("id")
      .description("Order id (UUID). A malformed id returns 400.")
      .example(
        OrderId.fromUuid(
          java.util.UUID.fromString("3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90")
        )
      )

  private def errorBody(
      what: String,
      example: ErrorResponse
  ): EndpointIO.Body[String, ErrorResponse] =
    jsonBody[ErrorResponse].description(what).example(example)

  private val notFoundVariant: EndpointOutput.OneOfVariant[OrderNotFound.type] =
    oneOfVariant(
      statusCode(StatusCode.NotFound)
        .and(errorBody("No order with this id.", notFound))
        .map[OrderNotFound.type](_ => OrderNotFound)(_ => notFound)
    )

  private val invalidStatusVariant: EndpointOutput.OneOfVariant[InvalidStatus] =
    oneOfVariant(
      statusCode(StatusCode.BadRequest)
        .and(
          errorBody(
            "`status` isn't a known order status.",
            ErrorResponse("Invalid order status: 'shipped!'")
          )
        )
        .map[InvalidStatus](e => InvalidStatus(e.error))(e =>
          ErrorResponse(s"Invalid order status: '${e.raw}'")
        )
    )

  private val emptyOrderItemsVariant
      : EndpointOutput.OneOfVariant[EmptyOrderItems.type] =
    oneOfVariant(
      statusCode(StatusCode.BadRequest)
        .and(
          errorBody("`items` is empty.", emptyItems)
        )
        .map[EmptyOrderItems.type](_ => EmptyOrderItems)(_ => emptyItems)
    )

  private val invalidOrderItemVariant
      : EndpointOutput.OneOfVariant[InvalidOrderItem] =
    oneOfVariant(
      statusCode(StatusCode.BadRequest)
        .and(
          errorBody(
            "A line's quantity is below 1 or its unit price below 0.",
            ErrorResponse(
              "quantity must be positive, got 0 for sku 'SKU-MUG-001'"
            )
          )
        )
        .map[InvalidOrderItem](e => InvalidOrderItem(e.error))(e =>
          ErrorResponse(e.reason)
        )
    )

  private val invalidCheckoutVariant
      : EndpointOutput.OneOfVariant[InvalidCheckout] =
    oneOfVariant(
      statusCode(StatusCode.BadRequest)
        .and(
          errorBody(
            "Invalid shipping address, a `paymentMethodId` that isn't a UUID or isn't known, or a total too large to represent.",
            ErrorResponse("paymentMethodId must be a UUID")
          )
        )
        .map(e => InvalidCheckout(e.error))(e => ErrorResponse(e.message))
    )

  private val illegalTransitionVariant
      : EndpointOutput.OneOfVariant[IllegalTransition] =
    oneOfVariant(
      statusCode(StatusCode.Conflict)
        .and(
          errorBody(
            "The order can't move to the requested status: it's past cancelling, or the target isn't `cancelled`.",
            ErrorResponse("Illegal status transition to 'dispatched'")
          )
        )
        .map[IllegalTransition](e => IllegalTransition(e.error))(e =>
          ErrorResponse(s"Illegal status transition to '${e.to}'")
        )
    )

  private val paymentDeclinedVariant
      : EndpointOutput.OneOfVariant[PaymentDeclined] =
    oneOfVariant(
      statusCode(StatusCode.PaymentRequired)
        .and(
          jsonBody[PaymentDeclinedResponse]
            .description(
              "The payment provider declined; any reserved stock was released."
            )
            .example(
              PaymentDeclinedResponse(
                "Payment declined",
                PaymentFailure("insufficient_funds")
              )
            )
        )
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
        .and(
          jsonBody[InsufficientStockResponse]
            .description(
              "One or more lines couldn't be reserved; nothing was reserved."
            )
            .example(
              InsufficientStockResponse(
                "Insufficient stock",
                List(ReservationFailureResponse("SKU-MUG-001", "out_of_stock"))
              )
            )
        )
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
      message: String,
      what: String
  ): EndpointOutput.OneOfVariant[E] = {
    val body = ErrorResponse(message)
    oneOfVariant(
      statusCode(status)
        .and(errorBody(what, body))
        .map(_ => value)(_ => body)
    )
  }

  // Each endpoint lists only the variants its logic can return, so `/docs`
  // doesn't advertise impossible errors. The variants themselves stay
  // defined once above. Same-status variants are told apart on encode by
  // type and on decode by order.
  private val createOrderErrorOutput: EndpointOutput[OrderError] =
    oneOf[OrderError](
      emptyOrderItemsVariant,
      invalidOrderItemVariant,
      invalidCheckoutVariant,
      paymentDeclinedVariant,
      insufficientStockVariant,
      fixedVariant(
        StatusCode.ServiceUnavailable,
        PaymentServiceUnavailable,
        "Payment service unavailable",
        "payment-service couldn't be reached; any reserved stock was released."
      ),
      fixedVariant(
        StatusCode.ServiceUnavailable,
        InventoryServiceUnavailable,
        "Inventory service unavailable",
        "inventory-service couldn't be reached; any reserved stock was released."
      ),
      fixedVariant(
        StatusCode.InternalServerError,
        OrderPersistFailed,
        "Order could not be saved",
        "Payment was authorized but the order couldn't be saved; stock was released and the authorization lapses."
      )
    )

  private val notFoundOutput: EndpointOutput[OrderError] =
    oneOf[OrderError](notFoundVariant)

  private val updateOrderErrorOutput: EndpointOutput[OrderError] =
    oneOf[OrderError](
      notFoundVariant,
      invalidStatusVariant,
      illegalTransitionVariant
    )

  private val cancelBody: EndpointIO.Body[String, UpdateOrderRequest] =
    jsonBody[UpdateOrderRequest].example(UpdateOrderRequest("cancelled"))

  private val createOrderEndpoint: PublicEndpoint[
    CreateOrderRequest,
    OrderError,
    OrderResponse,
    Any
  ] =
    endpoint.post
      .in("orders")
      .in(
        jsonBody[CreateOrderRequest].example(
          CreateOrderRequest(
            "7d1e4b2a-9c3f-4e8a-b5d6-1f2a3b4c5d6e",
            List(exampleItem),
            exampleAddress,
            "9b2e4c7a-1f3d-4a6b-8c5e-2d7f0a1b3c4e"
          )
        )
      )
      .out(statusCode(StatusCode.Created))
      .out(jsonBody[OrderResponse].example(exampleOrder))
      .errorOut(createOrderErrorOutput)
      .summary("Place an order")
      .description(
        "Checkout: validates the items, shipping address and payment method, reserves stock for every line, authorizes payment, then saves the order as `authorized`. Nothing is saved unless every step succeeds, and stock already reserved is released on failure. Not idempotent: each call places a new order."
      )
      .tag(tag)

  private val listOrdersEndpoint: PublicEndpoint[
    String,
    Unit,
    List[OrderResponse],
    Any
  ] =
    endpoint.get
      .in("orders")
      .in(
        query[String]("customerId")
          .description("Id (UUID) of the customer whose orders to list.")
          .example("7d1e4b2a-9c3f-4e8a-b5d6-1f2a3b4c5d6e")
      )
      .out(jsonBody[List[OrderResponse]].example(List(exampleOrder)))
      .summary("List a customer's orders")
      .description(
        "The customer's order history, newest first. Served from a short-TTL cache, so a just-placed order can take up to the TTL to appear."
      )
      .tag(tag)

  private val getOrderEndpoint: PublicEndpoint[
    OrderId,
    OrderError,
    OrderResponse,
    Any
  ] =
    endpoint.get
      .in("orders" / idPath)
      .out(jsonBody[OrderResponse].example(exampleOrder))
      .errorOut(notFoundOutput)
      .summary("Get an order")
      .description("One order with its line items.")
      .tag(tag)

  private val updateOrderEndpoint: PublicEndpoint[
    (OrderId, UpdateOrderRequest),
    OrderError,
    OrderResponse,
    Any
  ] =
    endpoint.patch
      .in("orders" / idPath)
      .in(cancelBody)
      .out(jsonBody[OrderResponse].example(exampleOrder))
      .errorOut(updateOrderErrorOutput)
      .summary("Cancel an order")
      .description(
        "Only `cancelled` is accepted; cancelling publishes `OrderCancelled`. Any other known status returns 409."
      )
      .tag(tag)

  private val replaceOrderEndpoint: PublicEndpoint[
    (OrderId, UpdateOrderRequest),
    OrderError,
    OrderResponse,
    Any
  ] =
    endpoint.put
      .in("orders" / idPath)
      .in(cancelBody)
      .out(jsonBody[OrderResponse].example(exampleOrder))
      .errorOut(updateOrderErrorOutput)
      .summary("Cancel an order (replace)")
      .description("Same as PATCH; kept for clients that use PUT.")
      .tag(tag)

  private val deleteOrderEndpoint
      : PublicEndpoint[OrderId, OrderError, Unit, Any] =
    endpoint.delete
      .in("orders" / idPath)
      .out(statusCode(StatusCode.NoContent))
      .errorOut(notFoundOutput)
      .summary("Delete an order")
      .description(
        "Hard-deletes the order and its lines. No event is published."
      )
      .tag(tag)

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
