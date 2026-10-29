package orderservice

import cats.effect.{Async, Ref, Resource, Sync}
import cats.effect.std.Console
import cats.syntax.all._
import fs2.io.net.Network
import io.circe.Json
import io.circe.parser.decode
import io.circe.syntax._
import orderservice.domain.{
  OrderEvent,
  OrderId,
  OrderItemId,
  OrderLifecycle,
  TransitionRejected
}
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.Meter
import skunk.{Codec, Session}
import skunk.codec.all._
import skunk.implicits._

import java.time.OffsetDateTime
import java.util.UUID
import scala.concurrent.duration.SECONDS

final case class Order(
    id: OrderId,
    customerId: String,
    totalCents: Int,
    status: OrderStatus,
    createdAt: java.time.Instant,
    updatedAt: java.time.Instant,
    // None only for orders created before US-11.1.
    shippingAddress: Option[Address] = None
)

final case class NewOrderItem(
    sku: String,
    productName: String,
    unitPriceCents: Int,
    quantity: Int
)

final case class OrderItem(
    id: OrderItemId,
    orderId: OrderId,
    sku: String,
    productName: String,
    unitPriceCents: Int,
    quantity: Int
)

/** Minimal projection of an order returned by `updateStatusIfCurrent` on a
  * successful transition - just the fields an outbox payload needs, fetched
  * atomically in the same query that does the update rather than a separate
  * round-trip.
  */
final case class UpdatedOrderRef(
    orderId: OrderId,
    customerId: String,
    totalCents: Int
)

/** Describes the outbox row a state-changing `OrderStore` call should write
  * atomically alongside its own UPDATE (ADR 0008, TD-3.2) - replacing the old
  * pattern of a separate, non-transactional `OrderEventPublisher` call after
  * the store returned. `payloadFor` is deferred rather than a pre-built `Json`
  * because `updateStatusIfCurrent` only learns the order's
  * id/customerId/totalCents from the UPDATE's own result (the caller doesn't
  * have them beforehand) - `update`'s caller does already have them, but takes
  * the same shape for one consistent API across both methods.
  */
final case class PendingOutboxEvent(
    eventType: String,
    payloadFor: UpdatedOrderRef => Json
)

trait OrderStore[F[_]] {

  /** Persists an already-authorized checkout (ADR 0018): ids are minted by the
    * caller before reserving/authorizing, so they're stored exactly as given,
    * order and items in one transaction. Writes no outbox row — payment-service
    * publishes `PaymentAuthorized`.
    */
  def createAuthorized(
      order: Order,
      items: List[OrderItem]
  ): F[(Order, List[OrderItem])]
  def get(id: OrderId): F[Option[(Order, List[OrderItem])]]
  def listByCustomer(customerId: String): F[List[(Order, List[OrderItem])]]

  /** The only status-changing API: applies `OrderLifecycle.transition` as an
    * atomic compare-and-set. `Left(UnknownOrder)` means no such row;
    * `Left(IllegalTransition)` means the row exists but is in the wrong state
    * (decided from a same-transaction status re-read, never guessed). On
    * success the outbox row, and any reconciliation flag, are written in the
    * same transaction.
    */
  def transition(
      id: OrderId,
      event: OrderEvent,
      outboxEvent: Option[PendingOutboxEvent] = None,
      reconciliationReason: Option[String] = None
  ): F[Either[TransitionRejected, UpdatedOrderRef]]

  /** Records a manual-reconciliation flag without changing status (used when a
    * `DispatchFailed` inventory release fails). Returns false for an unknown
    * order.
    */
  def flagReconciliation(id: OrderId, reason: String): F[Boolean]

  /** The manual-reconciliation reason recorded for an order, if any (US-19.3).
    */
  def reconciliationReason(id: OrderId): F[Option[String]]

  def delete(id: OrderId): F[Boolean]
  def ping: F[Boolean]
}

object OrderStore {

  private val eventSource = "order-service"

  /** Mints `eventId`/`timestamp` and assembles the full envelope - mirrors the
    * now-deleted `OrderEventPublisher.envelopeFor`, reused by both
    * `inMemory`/`inMemoryWithOutbox` and `postgres` below so the envelope shape
    * (per `system-design.md`'s "Envelope") is built in exactly one place.
    */
  private def buildEnvelope[F[_]: Sync](
      eventType: String,
      correlationId: String,
      payload: Json
  ): F[EventEnvelope] =
    for {
      eventId <- Sync[F].delay(UUID.randomUUID().toString)
      timestamp <- Sync[F].realTimeInstant
    } yield EventEnvelope(
      eventId = eventId,
      eventType = eventType,
      timestamp = timestamp,
      source = eventSource,
      correlationId = correlationId,
      payload = payload
    )

  // Column stays plain `text` at the DB level (hardened by a CHECK constraint,
  // not a native Postgres enum type - see V2__add_order_status_check.sql); this
  // eimap handles the Scala<->text mapping and surfaces a decode failure for
  // any value outside the closed OrderStatus set.
  private val orderStatus: Codec[OrderStatus] =
    text.eimap(OrderStatus.fromString)(_.asString)

  // Read as `shipping_address::text` and written as `$text::jsonb`, the same
  // no-skunk-circe approach as the outbox payload below.
  private val shippingAddress: Codec[Option[Address]] =
    text.opt.eimap(
      _.traverse(raw => decode[Address](raw).leftMap(_.getMessage))
    )(_.map(_.asJson.noSpaces))

  def inMemory[F[_]: Sync]: F[OrderStore[F]] = inMemoryWithOutbox[F].map(_._1)

  /** Same as `inMemory`, but also returns a `Ref` capturing every outbox write
    * (aggregateId, envelope) - for tests that need to assert a call site passed
    * the right `PendingOutboxEvent` through to the store, not just that it
    * stopped calling the old `OrderEventPublisher`. Kept as a separate
    * constructor (rather than changing `inMemory`'s own signature) so the ~40
    * existing `OrderStore.inMemory[IO]` call sites that don't care about outbox
    * behavior need no changes.
    */
  def inMemoryWithOutbox[F[_]: Sync]
      : F[(OrderStore[F], Ref[F, List[(String, EventEnvelope)]])] =
    for {
      ordersRef <- Ref.of[F, Map[OrderId, Order]](Map.empty)
      itemsRef <- Ref.of[F, Map[OrderId, List[OrderItem]]](Map.empty)
      outboxRef <- Ref.of[F, List[(String, EventEnvelope)]](Nil)
      reconciliationRef <- Ref.of[F, Map[OrderId, String]](Map.empty)
    } yield {
      def writeOutboxIfPresent(
          ref: UpdatedOrderRef,
          outboxEvent: Option[PendingOutboxEvent]
      ): F[Unit] =
        outboxEvent match {
          case None       => Sync[F].unit
          case Some(spec) =>
            buildEnvelope[F](
              spec.eventType,
              OrderId.render(ref.orderId),
              spec.payloadFor(ref)
            )
              .flatMap(envelope =>
                outboxRef.update(
                  _ :+ (OrderId.render(ref.orderId) -> envelope)
                )
              )
        }

      val store = new OrderStore[F] {
        def createAuthorized(
            order: Order,
            items: List[OrderItem]
        ): F[(Order, List[OrderItem])] =
          for {
            _ <- ordersRef.update(_ + (order.id -> order))
            _ <- itemsRef.update(_ + (order.id -> items))
          } yield (order, items)

        def get(id: OrderId): F[Option[(Order, List[OrderItem])]] =
          for {
            orders <- ordersRef.get
            items <- itemsRef.get
          } yield orders.get(id).map(order => (order, items.getOrElse(id, Nil)))

        def listByCustomer(
            customerId: String
        ): F[List[(Order, List[OrderItem])]] =
          for {
            orders <- ordersRef.get
            items <- itemsRef.get
          } yield orders.values
            .filter(_.customerId == customerId)
            .toList
            .sortBy(_.createdAt)(using Ordering[java.time.Instant].reverse)
            .map(order => (order, items.getOrElse(order.id, Nil)))

        def transition(
            id: OrderId,
            event: OrderEvent,
            outboxEvent: Option[PendingOutboxEvent] = None,
            reconciliationReason: Option[String] = None
        ): F[Either[TransitionRejected, UpdatedOrderRef]] =
          for {
            now <- Sync[F].realTimeInstant
            updated <- ordersRef.modify { entities =>
              entities.get(id) match {
                case None =>
                  (entities, Left(TransitionRejected.UnknownOrder))
                case Some(existing) =>
                  OrderLifecycle.transition(existing.status, event) match {
                    case Left(rejected)    => (entities, Left(rejected))
                    case Right(nextStatus) =>
                      val next =
                        existing.copy(status = nextStatus, updatedAt = now)
                      (
                        entities + (id -> next),
                        Right(
                          UpdatedOrderRef(
                            next.id,
                            next.customerId,
                            next.totalCents
                          )
                        )
                      )
                  }
              }
            }
            _ <- updated match {
              case Left(_)    => Sync[F].unit
              case Right(ref) =>
                writeOutboxIfPresent(ref, outboxEvent) *>
                  reconciliationReason.traverse_(reason =>
                    reconciliationRef.update(_ + (id -> reason))
                  )
            }
          } yield updated

        def flagReconciliation(id: OrderId, reason: String): F[Boolean] =
          ordersRef.get.map(_.contains(id)).flatMap {
            case true =>
              reconciliationRef.update(_ + (id -> reason)).as(true)
            case false => Sync[F].pure(false)
          }

        def reconciliationReason(id: OrderId): F[Option[String]] =
          reconciliationRef.get.map(_.get(id))

        def delete(id: OrderId): F[Boolean] =
          for {
            existed <- ordersRef.modify { entities =>
              if (entities.contains(id)) (entities - id, true)
              else (entities, false)
            }
            _ <- itemsRef.update(_ - id)
          } yield existed

        def ping: F[Boolean] = Sync[F].pure(true)
      }
      (store, outboxRef)
    }

  private val insertAuthorizedOrder: skunk.Query[
    (UUID, String, Int, OrderStatus, Option[Address]),
    (OffsetDateTime, OffsetDateTime)
  ] =
    sql"""
      INSERT INTO "order" (id, customer_id, total_cents, status, shipping_address)
      VALUES ($uuid, $text, $int4, $orderStatus, $shippingAddress::jsonb)
      RETURNING created_at, updated_at
    """.query(timestamptz *: timestamptz)

  private val selectOrder: skunk.Query[
    UUID,
    (
        String,
        Int,
        OrderStatus,
        OffsetDateTime,
        OffsetDateTime,
        Option[Address]
    )
  ] =
    sql"""
      SELECT customer_id, total_cents, status, created_at, updated_at, shipping_address::text
      FROM "order"
      WHERE id = $uuid
    """.query(
      text *: int4 *: orderStatus *: timestamptz *: timestamptz *: shippingAddress
    )

  private val selectOrdersByCustomer: skunk.Query[
    String,
    (UUID, Int, OrderStatus, OffsetDateTime, OffsetDateTime, Option[Address])
  ] =
    sql"""
      SELECT id, total_cents, status, created_at, updated_at, shipping_address::text
      FROM "order"
      WHERE customer_id = $text
      ORDER BY created_at DESC
    """.query(
      uuid *: int4 *: orderStatus *: timestamptz *: timestamptz *: shippingAddress
    )

  // Atomic guarded transition keyed by the order's own id - payment events
  // carry orderId.
  // Only transitions when the order's current status matches `expected`;
  // any other current status or an unknown id both no-op (0 rows).
  private val updateOrderStatusIfCurrent: skunk.Query[
    (OrderStatus, UUID, OrderStatus),
    (UUID, String, Int)
  ] =
    sql"""
      UPDATE "order"
      SET status = $orderStatus, updated_at = now()
      WHERE id = $uuid
        AND status = $orderStatus
      RETURNING id, customer_id, total_cents
    """.query(uuid *: text *: int4)

  // US-19.2/19.3: a capture outcome may arrive before or after the order's own
  // DispatchConfirmed consumer applies `authorized -> dispatched`, so either
  // pre-capture state is accepted.
  private val updateOrderStatusIfAwaitingCapture: skunk.Query[
    (OrderStatus, UUID, OrderStatus, OrderStatus),
    (UUID, String, Int)
  ] =
    sql"""
      UPDATE "order"
      SET status = $orderStatus, updated_at = now()
      WHERE id = $uuid
        AND status IN ($orderStatus, $orderStatus)
      RETURNING id, customer_id, total_cents
    """.query(uuid *: text *: int4)

  private val selectStatus: skunk.Query[UUID, OrderStatus] =
    sql"""SELECT status FROM "order" WHERE id = $uuid""".query(orderStatus)

  private val insertReconciliationIfOrderExists
      : skunk.Query[(UUID, String, UUID), UUID] =
    sql"""
      INSERT INTO order_reconciliation (order_id, reason)
      SELECT $uuid, $text
      WHERE EXISTS (SELECT 1 FROM "order" WHERE id = $uuid)
      ON CONFLICT (order_id) DO NOTHING
      RETURNING order_id
    """.query(uuid)

  private val insertReconciliation: skunk.Command[(UUID, String)] =
    sql"""
      INSERT INTO order_reconciliation (order_id, reason)
      VALUES ($uuid, $text)
      ON CONFLICT (order_id) DO NOTHING
    """.command

  private val selectReconciliationReason: skunk.Query[UUID, String] =
    sql"""
      SELECT reason FROM order_reconciliation WHERE order_id = $uuid
    """.query(text)

  // `payload` is bound as `$text` then cast `::jsonb` in-SQL - avoids adding
  // a skunk-circe dependency just for one write path that nothing in this
  // service ever reads back (only a future Debezium connector, TD-3.1,
  // does). `trace_parent` isn't set by any column here - no code anywhere
  // in this service propagates trace context into Kafka headers yet (see
  // ADR 0010's own note); it stays NULL at its column default.
  private val insertOutboxEvent: skunk.Command[(UUID, String, String, String)] =
    sql"""
      INSERT INTO outbox_events (id, aggregate_id, payload, correlation_id)
      VALUES ($uuid, $text, $text::jsonb, $text)
    """.command

  private val deleteOrder: skunk.Query[UUID, UUID] =
    sql"""
      DELETE FROM "order"
      WHERE id = $uuid
      RETURNING id
    """.query(uuid)

  private val insertOrderItem
      : skunk.Command[(UUID, UUID, String, String, Int, Int)] =
    sql"""
      INSERT INTO order_items (id, order_id, sku, product_name, unit_price_cents, quantity)
      VALUES ($uuid, $uuid, $text, $text, $int4, $int4)
    """.command

  private val selectOrderItems
      : skunk.Query[UUID, (UUID, String, String, Int, Int)] =
    sql"""
      SELECT id, sku, product_name, unit_price_cents, quantity
      FROM order_items
      WHERE order_id = $uuid
      ORDER BY sku
    """.query(uuid *: text *: text *: int4 *: int4)

  private val selectOrderItemsForOrders
      : skunk.Query[String, (UUID, UUID, String, String, Int, Int)] =
    sql"""
      SELECT order_id, id, sku, product_name, unit_price_cents, quantity
      FROM order_items
      WHERE order_id = ANY(string_to_array($text, ',')::uuid[])
      ORDER BY order_id, sku
    """.query(uuid *: uuid *: text *: text *: int4 *: int4)

  private val pingQuery: skunk.Query[skunk.Void, Int] = sql"SELECT 1".query(
    int4
  )

  def postgres[F[_]: Async: Console: Network](
      config: PostgresConfig,
      meter: Meter[F]
  ): Resource[F, OrderStore[F]] = {
    import org.typelevel.otel4s.trace.Tracer.Implicits.noop
    import org.typelevel.otel4s.metrics.Meter.Implicits.noop
    Session
      .Builder[F]
      .withHost(config.host)
      .withPort(config.port)
      .withUserAndPassword(config.user, config.password)
      .withDatabase(config.database)
      .pooled(max = 10)
      .evalMap { pool =>
        meter
          .histogram[Double]("db.client.operation.duration")
          .withUnit("s")
          .create
          .map { histogram =>
            /** Times a Skunk query, recording a `db.client.operation.duration`
              * measurement tagged with `db.system`/`db.operation` (OTel
              * semantic-convention names), plus `error.type` if it fails - this
              * is this service's only Postgres consumer, so it's instrumented
              * directly here rather than via a new purerest combinator.
              */
            def timed[A](operation: String)(fa: F[A]): F[A] =
              for {
                start <- Async[F].monotonic
                result <- fa.attempt
                end <- Async[F].monotonic
                outcomeAttributes = result match {
                  case Right(_)    => Nil
                  case Left(error) =>
                    List(Attribute("error.type", error.getClass.getName))
                }
                _ <- histogram.record(
                  (end - start).toUnit(SECONDS),
                  List(
                    Attribute("db.system", "postgresql"),
                    Attribute("db.operation", operation)
                  ) ++ outcomeAttributes
                )
                a <- result.liftTo[F]
              } yield a

            /** Writes the outbox row for `ref` using `session` - caller is
              * responsible for calling this from inside the same
              * `session.transaction.use` block as the state-change UPDATE it's
              * meant to be atomic with (ADR 0008's whole point: Kafka delivery
              * becomes a property of that one Postgres transaction committing).
              */
            def insertOutboxRow(
                session: Session[F],
                ref: UpdatedOrderRef,
                outboxEvent: PendingOutboxEvent
            ): F[Unit] =
              for {
                envelope <- buildEnvelope[F](
                  outboxEvent.eventType,
                  OrderId.render(ref.orderId),
                  outboxEvent.payloadFor(ref)
                )
                outboxId <- Sync[F].delay(UUID.randomUUID())
                _ <- session
                  .prepare(insertOutboxEvent)
                  .flatMap(
                    _.execute(
                      (
                        outboxId,
                        OrderId.render(ref.orderId),
                        envelope.asJson.noSpaces,
                        OrderId.render(ref.orderId)
                      )
                    )
                  )
              } yield ()

            def toItems(
                id: OrderId,
                rows: List[(UUID, String, String, Int, Int)]
            ): List[OrderItem] =
              rows.map { case (itemId, sku, name, price, qty) =>
                OrderItem(
                  OrderItemId.fromUuid(itemId),
                  id,
                  sku,
                  name,
                  price,
                  qty
                )
              }

            def toOrder(
                id: OrderId,
                row: (
                    String,
                    Int,
                    OrderStatus,
                    OffsetDateTime,
                    OffsetDateTime,
                    Option[Address]
                )
            ): Order = {
              val (
                customerId,
                totalCents,
                status,
                createdAt,
                updatedAt,
                address
              ) =
                row
              Order(
                id,
                customerId,
                totalCents,
                status,
                createdAt.toInstant,
                updatedAt.toInstant,
                address
              )
            }

            new OrderStore[F] {
              def createAuthorized(
                  order: Order,
                  items: List[OrderItem]
              ): F[(Order, List[OrderItem])] =
                timed("insert_authorized") {
                  pool.use { session =>
                    session.transaction.use { _ =>
                      for {
                        orderId <- Sync[F].delay(OrderId.uuid(order.id))
                        timestamps <- session
                          .prepare(insertAuthorizedOrder)
                          .flatMap(
                            _.unique(
                              (
                                orderId,
                                order.customerId,
                                order.totalCents,
                                order.status,
                                order.shippingAddress
                              )
                            )
                          )
                        insert <- session.prepare(insertOrderItem)
                        _ <- items.traverse_ { item =>
                          insert.execute(
                            (
                              OrderItemId.uuid(item.id),
                              orderId,
                              item.sku,
                              item.productName,
                              item.unitPriceCents,
                              item.quantity
                            )
                          )
                        }
                      } yield {
                        val (createdAt, updatedAt) = timestamps
                        (
                          order.copy(
                            createdAt = createdAt.toInstant,
                            updatedAt = updatedAt.toInstant
                          ),
                          items
                        )
                      }
                    }
                  }
                }

              def get(id: OrderId): F[Option[(Order, List[OrderItem])]] =
                timed("select") {
                  pool.use { session =>
                    val uuid = OrderId.uuid(id)
                    for {
                      row <- session
                        .prepare(selectOrder)
                        .flatMap(_.option(uuid))
                      items <- row.traverse(_ =>
                        session
                          .prepare(selectOrderItems)
                          .flatMap(_.stream(uuid, 64).compile.toList)
                          .map(toItems(id, _))
                      )
                    } yield row.map(r => (toOrder(id, r), items.getOrElse(Nil)))
                  }
                }

              def listByCustomer(
                  customerId: String
              ): F[List[(Order, List[OrderItem])]] =
                timed("select_by_customer") {
                  pool.use { session =>
                    session.execute(selectOrdersByCustomer)(customerId)
                  }
                }.flatMap { rows =>
                  val ids = rows.map(_._1)
                  if (ids.isEmpty) Sync[F].pure(Nil)
                  else
                    timed("select_items_batch") {
                      pool.use { session =>
                        session.execute(selectOrderItemsForOrders)(
                          ids.mkString(",")
                        )
                      }
                    }.map { itemRows =>
                      val grouped = itemRows
                        .groupBy(_._1)
                        .view
                        .mapValues(_.map {
                          case (orderUuid, iid, sku, name, price, qty) =>
                            OrderItem(
                              OrderItemId.fromUuid(iid),
                              OrderId.fromUuid(orderUuid),
                              sku,
                              name,
                              price,
                              qty
                            )
                        })
                        .toMap
                      rows.map {
                        case (id, total, status, created, updated, address) =>
                          (
                            Order(
                              OrderId.fromUuid(id),
                              customerId,
                              total,
                              status,
                              created.toInstant,
                              updated.toInstant,
                              address
                            ),
                            grouped.getOrElse(id, Nil)
                          )
                      }
                    }
                }

              def transition(
                  id: OrderId,
                  event: OrderEvent,
                  outboxEvent: Option[PendingOutboxEvent] = None,
                  reconciliationReason: Option[String] = None
              ): F[Either[TransitionRejected, UpdatedOrderRef]] = {
                val uuid = OrderId.uuid(id)
                val target = OrderLifecycle.target(event)
                val allowed = OrderLifecycle.allowedFrom(event)
                def ref(
                    row: (UUID, String, Int)
                ): UpdatedOrderRef = {
                  val (orderId, customerId, totalCents) = row
                  UpdatedOrderRef(
                    OrderId.fromUuid(orderId),
                    customerId,
                    totalCents
                  )
                }
                timed("transition") {
                  pool.use { session =>
                    session.transaction.use { _ =>
                      val updatedF: F[Option[UpdatedOrderRef]] =
                        if (allowed.size == 2) {
                          val statuses = allowed.toList.sortBy(_.asString)
                          session
                            .prepare(updateOrderStatusIfAwaitingCapture)
                            .flatMap(
                              _.option((target, uuid, statuses(0), statuses(1)))
                            )
                            .map(_.map(ref))
                        } else {
                          val only = allowed.head
                          session
                            .prepare(updateOrderStatusIfCurrent)
                            .flatMap(_.option((target, uuid, only)))
                            .map(_.map(ref))
                        }
                      for {
                        updated <- updatedF
                        result <- updated match {
                          case Some(updatedRef) =>
                            outboxEvent.traverse_(event =>
                              insertOutboxRow(session, updatedRef, event)
                            ) *>
                              reconciliationReason.traverse_(reason =>
                                session
                                  .prepare(insertReconciliation)
                                  .flatMap(_.execute((uuid, reason)))
                              ) *>
                              Sync[F].pure(
                                Right(
                                  updatedRef
                                ): Either[TransitionRejected, UpdatedOrderRef]
                              )
                          case None =>
                            session
                              .prepare(selectStatus)
                              .flatMap(_.option(uuid))
                              .map {
                                case None =>
                                  Left(TransitionRejected.UnknownOrder)
                                case Some(current) =>
                                  Left(
                                    TransitionRejected.IllegalTransition(
                                      current,
                                      target
                                    )
                                  )
                              }
                        }
                      } yield result
                    }
                  }
                }
              }

              def flagReconciliation(
                  id: OrderId,
                  reason: String
              ): F[Boolean] = {
                val uuid = OrderId.uuid(id)
                timed("flag_reconciliation") {
                  pool.use { session =>
                    session
                      .prepare(insertReconciliationIfOrderExists)
                      .flatMap(_.option((uuid, reason, uuid)))
                      .map(_.isDefined)
                  }
                }
              }

              def reconciliationReason(id: OrderId): F[Option[String]] =
                timed("select_reconciliation_reason") {
                  pool.use { session =>
                    session
                      .prepare(selectReconciliationReason)
                      .flatMap(_.option(OrderId.uuid(id)))
                  }
                }

              def delete(id: OrderId): F[Boolean] =
                timed("delete") {
                  pool.use { session =>
                    session
                      .prepare(deleteOrder)
                      .flatMap(_.option(OrderId.uuid(id)))
                      .map(_.isDefined)
                  }
                }

              def ping: F[Boolean] =
                timed("ping") {
                  pool.use(_.unique(pingQuery))
                }.attempt.map(_.isRight)
            }
          }
      }
  }
}
