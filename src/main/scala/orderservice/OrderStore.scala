package orderservice

import cats.effect.{Async, Ref, Resource, Sync}
import cats.effect.std.Console
import cats.syntax.all._
import fs2.io.net.Network
import io.circe.Json
import io.circe.parser.decode
import io.circe.syntax._
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
    id: String,
    orderId: String,
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
    orderId: String,
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
  def create(
      customerId: String,
      items: List[NewOrderItem]
  ): F[(Order, List[OrderItem])]

  /** Persists an already-authorized checkout (ADR 0018): ids are minted by the
    * caller before reserving/authorizing, so they're stored exactly as given,
    * order and items in one transaction. Writes no outbox row — payment-service
    * publishes `PaymentAuthorized`.
    */
  def createAuthorized(
      order: Order,
      items: List[OrderItem]
  ): F[(Order, List[OrderItem])]
  def get(id: String): F[Option[(Order, List[OrderItem])]]
  def listByCustomer(customerId: String): F[List[(Order, List[OrderItem])]]
  def update(
      id: String,
      status: OrderStatus,
      outboxEvent: Option[PendingOutboxEvent] = None
  ): F[Option[(Order, List[OrderItem])]]
  def updateStatusIfCurrent(
      id: String,
      expected: OrderStatus,
      newStatus: OrderStatus,
      outboxEvent: Option[PendingOutboxEvent] = None
  ): F[Option[UpdatedOrderRef]]
  def delete(id: String): F[Boolean]
  def ping: F[Boolean]
}

object OrderStore {

  private val defaultStatus = OrderStatus.Pending
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
      ordersRef <- Ref.of[F, Map[String, Order]](Map.empty)
      itemsRef <- Ref.of[F, Map[String, List[OrderItem]]](Map.empty)
      outboxRef <- Ref.of[F, List[(String, EventEnvelope)]](Nil)
    } yield {
      def writeOutboxIfPresent(
          ref: UpdatedOrderRef,
          outboxEvent: Option[PendingOutboxEvent]
      ): F[Unit] =
        outboxEvent match {
          case None       => Sync[F].unit
          case Some(spec) =>
            buildEnvelope[F](spec.eventType, ref.orderId, spec.payloadFor(ref))
              .flatMap(envelope =>
                outboxRef.update(_ :+ (ref.orderId -> envelope))
              )
        }

      val store = new OrderStore[F] {
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
              Sync[F].delay(java.util.UUID.randomUUID().toString).map {
                itemId =>
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

        def createAuthorized(
            order: Order,
            items: List[OrderItem]
        ): F[(Order, List[OrderItem])] =
          for {
            _ <- ordersRef.update(_ + (order.id -> order))
            _ <- itemsRef.update(_ + (order.id -> items))
          } yield (order, items)

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
            status: OrderStatus,
            outboxEvent: Option[PendingOutboxEvent] = None
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
            _ <- updated match {
              case None        => Sync[F].unit
              case Some(order) =>
                writeOutboxIfPresent(
                  UpdatedOrderRef(order.id, order.customerId, order.totalCents),
                  outboxEvent
                )
            }
            result <- updated match {
              case None        => Sync[F].pure(None)
              case Some(order) =>
                itemsRef.get.map(items =>
                  Some((order, items.getOrElse(id, Nil)))
                )
            }
          } yield result

        def updateStatusIfCurrent(
            id: String,
            expected: OrderStatus,
            newStatus: OrderStatus,
            outboxEvent: Option[PendingOutboxEvent] = None
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
            _ <- updated match {
              case None      => Sync[F].unit
              case Some(ref) => writeOutboxIfPresent(ref, outboxEvent)
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
      (store, outboxRef)
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

  private val updateOrder: skunk.Query[
    (OrderStatus, UUID),
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
      UPDATE "order"
      SET status = $orderStatus, updated_at = now()
      WHERE id = $uuid
      RETURNING customer_id, total_cents, status, created_at, updated_at, shipping_address::text
    """.query(
      text *: int4 *: orderStatus *: timestamptz *: timestamptz *: shippingAddress
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
                  ref.orderId,
                  outboxEvent.payloadFor(ref)
                )
                outboxId <- Sync[F].delay(UUID.randomUUID())
                _ <- session
                  .prepare(insertOutboxEvent)
                  .flatMap(
                    _.execute(
                      (
                        outboxId,
                        ref.orderId,
                        envelope.asJson.noSpaces,
                        ref.orderId
                      )
                    )
                  )
              } yield ()

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

              def createAuthorized(
                  order: Order,
                  items: List[OrderItem]
              ): F[(Order, List[OrderItem])] =
                timed("insert_authorized") {
                  pool.use { session =>
                    session.transaction.use { _ =>
                      for {
                        orderId <- Sync[F].delay(UUID.fromString(order.id))
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
                          Sync[F].delay(UUID.fromString(item.id)).flatMap {
                            itemId =>
                              insert.execute(
                                (
                                  itemId,
                                  orderId,
                                  item.sku,
                                  item.productName,
                                  item.unitPriceCents,
                                  item.quantity
                                )
                              )
                          }
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
                                updatedAt,
                                address
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
                                  updatedAt.toInstant,
                                  address
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
                    case (
                          id,
                          totalCents,
                          status,
                          createdAt,
                          updatedAt,
                          address
                        ) =>
                      fetchItems(id.toString, id).map { items =>
                        (
                          Order(
                            id.toString,
                            customerId,
                            totalCents,
                            status,
                            createdAt.toInstant,
                            updatedAt.toInstant,
                            address
                          ),
                          items
                        )
                      }
                  }
                }

              def update(
                  id: String,
                  status: OrderStatus,
                  outboxEvent: Option[PendingOutboxEvent] = None
              ): F[Option[(Order, List[OrderItem])]] =
                scala.util.Try(UUID.fromString(id)).toOption match {
                  case None       => Sync[F].pure(None)
                  case Some(uuid) =>
                    for {
                      row <- timed("update") {
                        pool.use { session =>
                          session.transaction.use { _ =>
                            for {
                              updated <- session
                                .prepare(updateOrder)
                                .flatMap(_.option((status, uuid)))
                              _ <- (updated, outboxEvent) match {
                                case (
                                      Some(
                                        (customerId, totalCents, _, _, _, _)
                                      ),
                                      Some(event)
                                    ) =>
                                  insertOutboxRow(
                                    session,
                                    UpdatedOrderRef(id, customerId, totalCents),
                                    event
                                  )
                                case _ => Sync[F].unit
                              }
                            } yield updated
                          }
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
                                updatedAt,
                                address
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
                                  updatedAt.toInstant,
                                  address
                                ),
                                items
                              )
                            )
                          }
                      }
                    } yield result
                }

              def updateStatusIfCurrent(
                  id: String,
                  expected: OrderStatus,
                  newStatus: OrderStatus,
                  outboxEvent: Option[PendingOutboxEvent] = None
              ): F[Option[UpdatedOrderRef]] =
                scala.util.Try(UUID.fromString(id)).toOption match {
                  case None       => Sync[F].pure(None)
                  case Some(uuid) =>
                    timed("update_status_if_current") {
                      pool.use { session =>
                        session.transaction.use { _ =>
                          for {
                            updated <- session
                              .prepare(updateOrderStatusIfCurrent)
                              .flatMap(_.option((newStatus, uuid, expected)))
                              .map(_.map {
                                case (orderId, customerId, totalCents) =>
                                  UpdatedOrderRef(
                                    orderId.toString,
                                    customerId,
                                    totalCents
                                  )
                              })
                            _ <- (updated, outboxEvent) match {
                              case (Some(ref), Some(event)) =>
                                insertOutboxRow(session, ref, event)
                              case _ => Sync[F].unit
                            }
                          } yield updated
                        }
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
