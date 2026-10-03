package orderservice

import cats.effect.{Async, Ref, Resource, Sync}
import cats.effect.std.Console
import cats.syntax.all._
import fs2.io.net.Network
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.Meter
import skunk.{Codec, Session}
import skunk.codec.all._
import skunk.implicits._

import java.time.OffsetDateTime
import java.util.UUID
import scala.concurrent.duration.SECONDS

final case class Order(
    id: String,
    customerId: String,
    totalCents: Int,
    status: OrderStatus,
    createdAt: java.time.Instant,
    updatedAt: java.time.Instant
)

final case class NewOrderItem(
    sku: String,
    productName: String,
    unitPriceCents: Int,
    quantity: Int
)

final case class OrderItem(
    id: String,
    orderId: String,
    sku: String,
    productName: String,
    unitPriceCents: Int,
    quantity: Int
)

/** Minimal projection of an order returned by `updateStatusByItemId` on a
  * successful transition - just the fields `order.reserved`'s payload needs
  * (see US-5.3's spec.md), fetched atomically in the same query that does the
  * update rather than a separate round-trip.
  */
final case class UpdatedOrderRef(
    orderId: String,
    customerId: String,
    totalCents: Int
)

trait OrderStore[F[_]] {
  def create(
      customerId: String,
      items: List[NewOrderItem]
  ): F[(Order, List[OrderItem])]
  def get(id: String): F[Option[(Order, List[OrderItem])]]
  def listByCustomer(customerId: String): F[List[(Order, List[OrderItem])]]
  def update(
      id: String,
      status: OrderStatus
  ): F[Option[(Order, List[OrderItem])]]
  def updateStatusByItemId(
      orderItemId: String,
      newStatus: OrderStatus
  ): F[Option[UpdatedOrderRef]]
  def updateStatusIfCurrent(
      id: String,
      expected: OrderStatus,
      newStatus: OrderStatus
  ): F[Option[UpdatedOrderRef]]
  def delete(id: String): F[Boolean]
  def ping: F[Boolean]
}

object OrderStore {

  private val defaultStatus = OrderStatus.Pending

  // Column stays plain `text` at the DB level (hardened by a CHECK constraint,
  // not a native Postgres enum type - see V2__add_order_status_check.sql); this
  // eimap handles the Scala<->text mapping and surfaces a decode failure for
  // any value outside the closed OrderStatus set.
  private val orderStatus: Codec[OrderStatus] =
    text.eimap(OrderStatus.fromString)(_.asString)

  def inMemory[F[_]: Sync]: F[OrderStore[F]] =
    for {
      ordersRef <- Ref.of[F, Map[String, Order]](Map.empty)
      itemsRef <- Ref.of[F, Map[String, List[OrderItem]]](Map.empty)
    } yield new OrderStore[F] {
      def create(
          customerId: String,
          items: List[NewOrderItem]
      ): F[(Order, List[OrderItem])] =
        for {
          id <- Sync[F].delay(java.util.UUID.randomUUID().toString)
          now <- Sync[F].realTimeInstant
          totalCents = items.map(i => i.unitPriceCents * i.quantity).sum
          entity = Order(id, customerId, totalCents, defaultStatus, now, now)
          persistedItems <- items.traverse { item =>
            Sync[F].delay(java.util.UUID.randomUUID().toString).map { itemId =>
              OrderItem(
                itemId,
                id,
                item.sku,
                item.productName,
                item.unitPriceCents,
                item.quantity
              )
            }
          }
          _ <- ordersRef.update(_ + (id -> entity))
          _ <- itemsRef.update(_ + (id -> persistedItems))
        } yield (entity, persistedItems)

      def get(id: String): F[Option[(Order, List[OrderItem])]] =
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

      def update(
          id: String,
          status: OrderStatus
      ): F[Option[(Order, List[OrderItem])]] =
        for {
          now <- Sync[F].realTimeInstant
          updated <- ordersRef.modify { entities =>
            entities.get(id) match {
              case None           => (entities, None)
              case Some(existing) =>
                val next =
                  existing.copy(
                    status = status,
                    updatedAt = now
                  )
                (entities + (id -> next), Some(next))
            }
          }
          result <- updated match {
            case None        => Sync[F].pure(None)
            case Some(order) =>
              itemsRef.get.map(items => Some((order, items.getOrElse(id, Nil))))
          }
        } yield result

      def updateStatusByItemId(
          orderItemId: String,
          newStatus: OrderStatus
      ): F[Option[UpdatedOrderRef]] =
        for {
          items <- itemsRef.get
          maybeOrderId = items.collectFirst {
            case (orderId, orderItems)
                if orderItems.exists(_.id == orderItemId) =>
              orderId
          }
          now <- Sync[F].realTimeInstant
          updated <- maybeOrderId match {
            case None          => Sync[F].pure(None)
            case Some(orderId) =>
              ordersRef.modify { entities =>
                entities.get(orderId) match {
                  case Some(existing)
                      if existing.status == OrderStatus.Pending =>
                    val next =
                      existing.copy(status = newStatus, updatedAt = now)
                    (
                      entities + (orderId -> next),
                      Some(
                        UpdatedOrderRef(
                          next.id,
                          next.customerId,
                          next.totalCents
                        )
                      )
                    )
                  case _ => (entities, None)
                }
              }
          }
        } yield updated

      def updateStatusIfCurrent(
          id: String,
          expected: OrderStatus,
          newStatus: OrderStatus
      ): F[Option[UpdatedOrderRef]] =
        for {
          now <- Sync[F].realTimeInstant
          updated <- ordersRef.modify { entities =>
            entities.get(id) match {
              case Some(existing) if existing.status == expected =>
                val next = existing.copy(status = newStatus, updatedAt = now)
                (
                  entities + (id -> next),
                  Some(
                    UpdatedOrderRef(next.id, next.customerId, next.totalCents)
                  )
                )
              case _ => (entities, None)
            }
          }
        } yield updated

      def delete(id: String): F[Boolean] =
        for {
          existed <- ordersRef.modify { entities =>
            if (entities.contains(id)) (entities - id, true)
            else (entities, false)
          }
          _ <- itemsRef.update(_ - id)
        } yield existed

      def ping: F[Boolean] = Sync[F].pure(true)
    }

  private val insertOrder: skunk.Query[
    (UUID, String, Int, OrderStatus),
    (OffsetDateTime, OffsetDateTime)
  ] =
    sql"""
      INSERT INTO "order" (id, customer_id, total_cents, status)
      VALUES ($uuid, $text, $int4, $orderStatus)
      RETURNING created_at, updated_at
    """.query(timestamptz *: timestamptz)

  private val selectOrder: skunk.Query[
    UUID,
    (String, Int, OrderStatus, OffsetDateTime, OffsetDateTime)
  ] =
    sql"""
      SELECT customer_id, total_cents, status, created_at, updated_at
      FROM "order"
      WHERE id = $uuid
    """.query(text *: int4 *: orderStatus *: timestamptz *: timestamptz)

  private val selectOrdersByCustomer: skunk.Query[
    String,
    (UUID, Int, OrderStatus, OffsetDateTime, OffsetDateTime)
  ] =
    sql"""
      SELECT id, total_cents, status, created_at, updated_at
      FROM "order"
      WHERE customer_id = $text
      ORDER BY created_at DESC
    """.query(uuid *: int4 *: orderStatus *: timestamptz *: timestamptz)

  private val updateOrder: skunk.Query[
    (OrderStatus, UUID),
    (String, Int, OrderStatus, OffsetDateTime, OffsetDateTime)
  ] =
    sql"""
      UPDATE "order"
      SET status = $orderStatus, updated_at = now()
      WHERE id = $uuid
      RETURNING customer_id, total_cents, status, created_at, updated_at
    """.query(text *: int4 *: orderStatus *: timestamptz *: timestamptz)

  // Joins through order_items to find the owning order, atomically in one
  // statement - avoids a separate lookup + check + update race (two
  // concurrent events for different items of the same order could otherwise
  // interleave). Only transitions a Pending order; already-resolved orders
  // or an unknown orderItemId both no-op (0 rows), surfaced as `false`.
  private val updateOrderStatusByItemId: skunk.Query[
    (OrderStatus, OrderStatus, UUID),
    (UUID, String, Int)
  ] =
    sql"""
      UPDATE "order"
      SET status = $orderStatus, updated_at = now()
      WHERE status = $orderStatus
        AND id = (SELECT order_id FROM order_items WHERE id = $uuid)
      RETURNING id, customer_id, total_cents
    """.query(uuid *: text *: int4)

  // Atomic guarded transition keyed by the order's own id directly (unlike
  // updateOrderStatusByItemId above, which joins through order_items) -
  // payment.settled/payment.failed events carry orderId, not an item id.
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

            def fetchItems(id: String, uuid: UUID): F[List[OrderItem]] =
              timed("select_items") {
                pool.use { session => session.execute(selectOrderItems)(uuid) }
              }.map(_.map {
                case (itemId, sku, productName, unitPriceCents, quantity) =>
                  OrderItem(
                    itemId.toString,
                    id,
                    sku,
                    productName,
                    unitPriceCents,
                    quantity
                  )
              })

            new OrderStore[F] {
              def create(
                  customerId: String,
                  items: List[NewOrderItem]
              ): F[(Order, List[OrderItem])] = {
                val totalCents =
                  items.map(i => i.unitPriceCents * i.quantity).sum
                timed("insert") {
                  pool.use { session =>
                    session.transaction.use { _ =>
                      for {
                        id <- Sync[F].delay(UUID.randomUUID())
                        timestamps <- session
                          .prepare(insertOrder)
                          .flatMap(
                            _.unique(
                              (id, customerId, totalCents, defaultStatus)
                            )
                          )
                        persistedItems <- items.traverse { item =>
                          for {
                            itemId <- Sync[F].delay(UUID.randomUUID())
                            _ <- session
                              .prepare(insertOrderItem)
                              .flatMap(
                                _.execute(
                                  (
                                    itemId,
                                    id,
                                    item.sku,
                                    item.productName,
                                    item.unitPriceCents,
                                    item.quantity
                                  )
                                )
                              )
                          } yield OrderItem(
                            itemId.toString,
                            id.toString,
                            item.sku,
                            item.productName,
                            item.unitPriceCents,
                            item.quantity
                          )
                        }
                      } yield {
                        val (createdAt, updatedAt) = timestamps
                        (
                          Order(
                            id.toString,
                            customerId,
                            totalCents,
                            defaultStatus,
                            createdAt.toInstant,
                            updatedAt.toInstant
                          ),
                          persistedItems
                        )
                      }
                    }
                  }
                }
              }

              def get(id: String): F[Option[(Order, List[OrderItem])]] =
                scala.util.Try(UUID.fromString(id)).toOption match {
                  case None       => Sync[F].pure(None)
                  case Some(uuid) =>
                    for {
                      row <- timed("select") {
                        pool.use { session =>
                          session.prepare(selectOrder).flatMap(_.option(uuid))
                        }
                      }
                      result <- row match {
                        case None => Sync[F].pure(None)
                        case Some(
                              (
                                customerId,
                                totalCents,
                                status,
                                createdAt,
                                updatedAt
                              )
                            ) =>
                          fetchItems(id, uuid).map { items =>
                            Some(
                              (
                                Order(
                                  id,
                                  customerId,
                                  totalCents,
                                  status,
                                  createdAt.toInstant,
                                  updatedAt.toInstant
                                ),
                                items
                              )
                            )
                          }
                      }
                    } yield result
                }

              def listByCustomer(
                  customerId: String
              ): F[List[(Order, List[OrderItem])]] =
                timed("select_by_customer") {
                  pool.use { session =>
                    session.execute(selectOrdersByCustomer)(customerId)
                  }
                }.flatMap { rows =>
                  rows.traverse {
                    case (id, totalCents, status, createdAt, updatedAt) =>
                      fetchItems(id.toString, id).map { items =>
                        (
                          Order(
                            id.toString,
                            customerId,
                            totalCents,
                            status,
                            createdAt.toInstant,
                            updatedAt.toInstant
                          ),
                          items
                        )
                      }
                  }
                }

              def update(
                  id: String,
                  status: OrderStatus
              ): F[Option[(Order, List[OrderItem])]] =
                scala.util.Try(UUID.fromString(id)).toOption match {
                  case None       => Sync[F].pure(None)
                  case Some(uuid) =>
                    for {
                      row <- timed("update") {
                        pool.use { session =>
                          session
                            .prepare(updateOrder)
                            .flatMap(_.option((status, uuid)))
                        }
                      }
                      result <- row match {
                        case None => Sync[F].pure(None)
                        case Some(
                              (
                                customerId,
                                totalCents,
                                newStatus,
                                createdAt,
                                updatedAt
                              )
                            ) =>
                          fetchItems(id, uuid).map { items =>
                            Some(
                              (
                                Order(
                                  id,
                                  customerId,
                                  totalCents,
                                  newStatus,
                                  createdAt.toInstant,
                                  updatedAt.toInstant
                                ),
                                items
                              )
                            )
                          }
                      }
                    } yield result
                }

              def updateStatusByItemId(
                  orderItemId: String,
                  newStatus: OrderStatus
              ): F[Option[UpdatedOrderRef]] =
                scala.util.Try(UUID.fromString(orderItemId)).toOption match {
                  case None           => Sync[F].pure(None)
                  case Some(itemUuid) =>
                    timed("update_status_by_item_id") {
                      pool.use { session =>
                        session
                          .prepare(updateOrderStatusByItemId)
                          .flatMap(
                            _.option(
                              (newStatus, OrderStatus.Pending, itemUuid)
                            )
                          )
                          .map(_.map { case (orderId, customerId, totalCents) =>
                            UpdatedOrderRef(
                              orderId.toString,
                              customerId,
                              totalCents
                            )
                          })
                      }
                    }
                }

              def updateStatusIfCurrent(
                  id: String,
                  expected: OrderStatus,
                  newStatus: OrderStatus
              ): F[Option[UpdatedOrderRef]] =
                scala.util.Try(UUID.fromString(id)).toOption match {
                  case None       => Sync[F].pure(None)
                  case Some(uuid) =>
                    timed("update_status_if_current") {
                      pool.use { session =>
                        session
                          .prepare(updateOrderStatusIfCurrent)
                          .flatMap(_.option((newStatus, uuid, expected)))
                          .map(_.map { case (orderId, customerId, totalCents) =>
                            UpdatedOrderRef(
                              orderId.toString,
                              customerId,
                              totalCents
                            )
                          })
                      }
                    }
                }

              def delete(id: String): F[Boolean] =
                scala.util.Try(UUID.fromString(id)).toOption match {
                  case None       => Sync[F].pure(false)
                  case Some(uuid) =>
                    timed("delete") {
                      pool.use { session =>
                        session
                          .prepare(deleteOrder)
                          .flatMap(_.option(uuid))
                          .map(_.isDefined)
                      }
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
