package orderservice

import cats.effect.{IO, Ref}

import java.time.Instant
import java.util.UUID

/** Shared checkout test doubles (ADR 0018): default clients that always
  * reserve/authorize, plus recording variants for asserting which downstream
  * calls a checkout made.
  */
object CheckoutFixtures {

  val shippingAddress: Address =
    Address("1 High St", None, "London", "SW1A 1AA", "GB")

  val paymentMethodId: String = "6f1c2c1e-6a43-4d2b-9a55-2f3f6c1b9b10"

  def createRequest(
      items: List[CreateOrderItemRequest] = List(
        CreateOrderItemRequest("sku-1", "Widget", 999, 5)
      ),
      customerId: String = "cust-123"
  ): CreateOrderRequest =
    CreateOrderRequest(customerId, items, shippingAddress, paymentMethodId)

  def inventoryClient(
      reserve: IO[OrderReservationResult] =
        IO.pure(OrderReservationResult.Reserved),
      release: IO[Unit] = IO.unit
  ): InventoryClient[IO] = {
    val reserveResult = reserve
    val releaseResult = release
    new InventoryClient[IO] {
      def reserveOrder(
          orderId: String,
          items: List[OrderReservationLine]
      ): IO[OrderReservationResult] = reserveResult
      def release(orderId: String): IO[Unit] = releaseResult
    }
  }

  val alwaysReserves: InventoryClient[IO] = inventoryClient()

  def paymentClient(
      result: IO[AuthorizationResult] = IO.pure(
        AuthorizationResult.Authorized(
          "auth-1",
          Instant.parse("2026-11-03T12:00:00Z")
        )
      )
  ): PaymentClient[IO] =
    new PaymentClient[IO] {
      def authorize(
          orderId: String,
          amountCents: Int,
          paymentMethodId: String
      ): IO[AuthorizationResult] = result
    }

  val alwaysAuthorizes: PaymentClient[IO] = paymentClient()

  sealed trait Call
  final case class Reserve(orderId: String, items: List[OrderReservationLine])
      extends Call
  final case class Release(orderId: String) extends Call
  final case class Authorize(
      orderId: String,
      amountCents: Int,
      paymentMethodId: String
  ) extends Call

  /** Wraps both clients so every call lands, in order, in `calls`. */
  def recording(
      calls: Ref[IO, List[Call]],
      inventory: InventoryClient[IO],
      payment: PaymentClient[IO]
  ): (InventoryClient[IO], PaymentClient[IO]) =
    (
      new InventoryClient[IO] {
        def reserveOrder(
            orderId: String,
            items: List[OrderReservationLine]
        ): IO[OrderReservationResult] =
          calls.update(_ :+ Reserve(orderId, items)) *>
            inventory.reserveOrder(orderId, items)
        def release(orderId: String): IO[Unit] =
          calls.update(_ :+ Release(orderId)) *> inventory.release(orderId)
      },
      new PaymentClient[IO] {
        def authorize(
            orderId: String,
            amountCents: Int,
            paymentMethodId: String
        ): IO[AuthorizationResult] =
          calls.update(_ :+ Authorize(orderId, amountCents, paymentMethodId)) *>
            payment.authorize(orderId, amountCents, paymentMethodId)
      }
    )

  def isUuid(value: String): Boolean =
    scala.util.Try(UUID.fromString(value)).isSuccess
}
