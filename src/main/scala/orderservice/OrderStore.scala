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

trait OrderStore[F[_]] {
  def create(
      customerId: String,
      items: List[NewOrderItem]
  ): F[(Order, List[OrderItem])]
  def get(id: String): F[Option[(Order, List[OrderItem])]]
  def update(
      id: String,
      status: OrderStatus
  ): F[Option[(Order, List[OrderItem])]]
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
