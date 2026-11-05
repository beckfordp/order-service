package orderservice

import cats.FlatMap
import cats.effect.{Async, Ref, Resource, Sync}
import cats.effect.std.Console
import cats.syntax.all._
import fs2.io.net.Network
import io.circe.Json
import io.circe.parser.decode
import io.circe.syntax._
import orderservice.domain.{
  CancellationAudit,
  OrderCancellation,
  OrderEvent,
  OrderId,
  OrderItemId,
  OrderLifecycle,
  Reconciliation,
  ReconciliationStatus,
  ResolveReconciliationOutcome,
  TransitionRejected
}
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.Meter
import org.typelevel.otel4s.trace.Tracer
import purekafka.tracing.KafkaTracing
import purerest.metrics.{LatencyBuckets, PoolMetrics}
import skunk.{Codec, Session}
import skunk.codec.all._
import skunk.implicits._

import java.time.{Instant, OffsetDateTime, ZoneOffset}
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
    shippingAddress: Option[Address] = None,
    // ADM-10: the guest checkout's address (no user-service account).
    guestEmail: Option[String] = None
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
  * round-trip. `from`/`enteredAt` are the status left and when it was entered
  * (its `updated_at`), for the transition and time-in-state metrics
  * (`JourneyMetrics`).
  */
final case class UpdatedOrderRef(
    orderId: OrderId,
    customerId: String,
    totalCents: Int,
    from: OrderStatus,
    enteredAt: java.time.Instant
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

  /** Admin-wide order list (ADM-4): every order, optional status/customer
    * filters, newest first. Deliberately not served by the customer history
    * cache.
    */
  def listAll(
      status: Option[OrderStatus],
      customerId: Option[String]
  ): F[List[(Order, List[OrderItem])]]

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

  /** The open/resolved reconciliation queue (ADM-4). Both filters optional and
    * ANDed; newest first.
    */
  def listReconciliations(
      status: Option[ReconciliationStatus],
      orderId: Option[OrderId]
  ): F[List[Reconciliation]]

  /** Resolves an open flag, recording the operator and note. The first resolver
    * wins; a repeat returns the existing row untouched.
    */
  def resolveReconciliation(
      id: OrderId,
      operatorId: String,
      note: Option[String],
      resolvedAt: Instant
  ): F[ResolveReconciliationOutcome]

  /** The cancellation actor/reason recorded for an order, if any (US-22). */
  def cancellationAudit(id: OrderId): F[Option[CancellationAudit]]

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

  private val guestEmail: Codec[Option[String]] = text.opt

  private val instant: Codec[Instant] =
    timestamptz.eimap(odt => Right(odt.toInstant))(instant =>
      OffsetDateTime.ofInstant(instant, ZoneOffset.UTC)
    )

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
      reconciliationRef <- Ref.of[F, Map[OrderId, Reconciliation]](Map.empty)
      auditRef <- Ref.of[F, Map[OrderId, CancellationAudit]](Map.empty)
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
                            next.totalCents,
                            existing.status,
                            existing.updatedAt
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
                    reconciliationRef.update(reconciliations =>
                      if (reconciliations.contains(id)) reconciliations
                      else
                        reconciliations + (id -> Reconciliation(
                          id,
                          reason,
                          ReconciliationStatus.Open,
                          now
                        ))
                    )
                  ) *>
                  OrderCancellation
                    .auditFor(event, ref, now)
                    .traverse_(audit =>
                      auditRef.update(audits => audits + (id -> audit))
                    )
            }
          } yield updated

        def flagReconciliation(id: OrderId, reason: String): F[Boolean] =
          Sync[F].realTimeInstant.flatMap { now =>
            ordersRef.get.map(_.contains(id)).flatMap {
              case true =>
                reconciliationRef
                  .update(reconciliations =>
                    if (reconciliations.contains(id)) reconciliations
                    else
                      reconciliations + (id -> Reconciliation(
                        id,
                        reason,
                        ReconciliationStatus.Open,
                        now
                      ))
                  )
                  .as(true)
              case false => Sync[F].pure(false)
            }
          }

        def reconciliationReason(id: OrderId): F[Option[String]] =
          reconciliationRef.get.map(_.get(id).map(_.reason))

        def listAll(
            status: Option[OrderStatus],
            customerId: Option[String]
        ): F[List[(Order, List[OrderItem])]] =
          for {
            orders <- ordersRef.get
            items <- itemsRef.get
          } yield orders.values
            .filter(order => status.forall(_ == order.status))
            .filter(order => customerId.forall(_ == order.customerId))
            .toList
            .sortBy(order => (order.createdAt, OrderId.render(order.id)))(using
              Ordering[(Instant, String)].reverse
            )
            .map(order => (order, items.getOrElse(order.id, Nil)))

        def listReconciliations(
            status: Option[ReconciliationStatus],
            orderId: Option[OrderId]
        ): F[List[Reconciliation]] =
          reconciliationRef.get.map(
            _.values
              .filter(row => status.forall(_ == row.status))
              .filter(row => orderId.forall(_ == row.orderId))
              .toList
              .sortBy(row => (row.createdAt, OrderId.render(row.orderId)))(using
                Ordering[(Instant, String)].reverse
              )
          )

        def resolveReconciliation(
            id: OrderId,
            operatorId: String,
            note: Option[String],
            resolvedAt: Instant
        ): F[ResolveReconciliationOutcome] =
          reconciliationRef.modify { rows =>
            rows.get(id) match {
              case None      => (rows, ResolveReconciliationOutcome.NotFound)
              case Some(row) =>
                row.resolve(operatorId, note, resolvedAt) match {
                  case Some(resolved) =>
                    (
                      rows.updated(id, resolved),
                      ResolveReconciliationOutcome.Resolved(resolved)
                    )
                  case None =>
                    (
                      rows,
                      ResolveReconciliationOutcome.AlreadyResolved(row)
                    )
                }
            }
          }

        def cancellationAudit(id: OrderId): F[Option[CancellationAudit]] =
          auditRef.get.map(_.get(id))

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
    (UUID, String, Int, OrderStatus, Option[Address], Option[String]),
    (OffsetDateTime, OffsetDateTime)
  ] =
    sql"""
      INSERT INTO "order" (id, customer_id, total_cents, status, shipping_address, guest_email)
      VALUES ($uuid, $text, $int4, $orderStatus, $shippingAddress::jsonb, $guestEmail)
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
        Option[Address],
        Option[String]
    )
  ] =
    sql"""
      SELECT customer_id, total_cents, status, created_at, updated_at, shipping_address::text,
             guest_email
      FROM "order"
      WHERE id = $uuid
    """.query(
      text *: int4 *: orderStatus *: timestamptz *: timestamptz *:
        shippingAddress *: guestEmail
    )

  private val selectOrdersByCustomer: skunk.Query[
    String,
    (
        UUID,
        Int,
        OrderStatus,
        OffsetDateTime,
        OffsetDateTime,
        Option[Address],
        Option[String]
    )
  ] =
    sql"""
      SELECT id, total_cents, status, created_at, updated_at, shipping_address::text,
             guest_email
      FROM "order"
      WHERE customer_id = $text
      ORDER BY created_at DESC
    """.query(
      uuid *: int4 *: orderStatus *: timestamptz *: timestamptz *:
        shippingAddress *: guestEmail
    )

  private val optionalOrderStatus: Codec[Option[OrderStatus]] =
    orderStatus.opt

  // Interpolated by name: `$text.opt` would bind `.opt` as SQL text, not as a
  // codec modifier, so optional parameters use this codec directly.
  private val optionalText: Codec[Option[String]] = text.opt

  private val selectAllOrders: skunk.Query[
    (Option[OrderStatus], Option[OrderStatus], Option[String], Option[String]),
    (
        UUID,
        String,
        Int,
        OrderStatus,
        OffsetDateTime,
        OffsetDateTime,
        Option[Address],
        Option[String]
    )
  ] =
    sql"""
      SELECT id, customer_id, total_cents, status, created_at, updated_at,
             shipping_address::text, guest_email
      FROM "order"
      WHERE ($optionalOrderStatus IS NULL OR status = $optionalOrderStatus)
        AND ($optionalText IS NULL OR customer_id = $optionalText)
      ORDER BY created_at DESC, id
    """.query(
      uuid *: text *: int4 *: orderStatus *: timestamptz *: timestamptz *:
        shippingAddress *: guestEmail
    )

  // Atomic guarded transition keyed by the order's own id - payment events
  // carry orderId.
  // Only transitions when the order's current status matches `expected`;
  // any other current status or an unknown id both no-op (0 rows). The
  // `prev` CTE returns the pre-update status/updated_at (Postgres 16 has no
  // `RETURNING OLD`); `FOR UPDATE` makes it read the latest committed row, not
  // the statement snapshot, when a concurrent transition got there first.
  private val updateOrderStatusIfCurrent: skunk.Query[
    (UUID, OrderStatus, OrderStatus),
    (UUID, String, Int, OrderStatus, OffsetDateTime)
  ] =
    sql"""
      WITH prev AS (
        SELECT id, status, updated_at FROM "order" WHERE id = $uuid FOR UPDATE
      )
      UPDATE "order" o
      SET status = $orderStatus, updated_at = now()
      FROM prev
      WHERE o.id = prev.id
        AND o.status = $orderStatus
      RETURNING o.id, o.customer_id, o.total_cents, prev.status, prev.updated_at
    """.query(uuid *: text *: int4 *: orderStatus *: timestamptz)

  // US-19.2/19.3: a capture outcome may arrive before or after the order's own
  // DispatchConfirmed consumer applies `authorized -> dispatched`, so either
  // pre-capture state is accepted.
  private val updateOrderStatusIfAwaitingCapture: skunk.Query[
    (UUID, OrderStatus, OrderStatus, OrderStatus),
    (UUID, String, Int, OrderStatus, OffsetDateTime)
  ] =
    sql"""
      WITH prev AS (
        SELECT id, status, updated_at FROM "order" WHERE id = $uuid FOR UPDATE
      )
      UPDATE "order" o
      SET status = $orderStatus, updated_at = now()
      FROM prev
      WHERE o.id = prev.id
        AND o.status IN ($orderStatus, $orderStatus)
      RETURNING o.id, o.customer_id, o.total_cents, prev.status, prev.updated_at
    """.query(uuid *: text *: int4 *: orderStatus *: timestamptz)

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

  private val insertReconciliation: skunk.Query[(UUID, String), UUID] =
    sql"""
      INSERT INTO order_reconciliation (order_id, reason)
      VALUES ($uuid, $text)
      ON CONFLICT (order_id) DO NOTHING
      RETURNING order_id
    """.query(uuid)

  private val selectReconciliationReason: skunk.Query[UUID, String] =
    sql"""
      SELECT reason FROM order_reconciliation WHERE order_id = $uuid
    """.query(text)

  private val reconciliationStatusCodec: Codec[ReconciliationStatus] =
    text.eimap(ReconciliationStatus.fromString)(_.asString)

  private val reconciliationRow: Codec[
    (
        UUID,
        String,
        ReconciliationStatus,
        OffsetDateTime,
        Option[String],
        Option[String],
        Option[OffsetDateTime]
    )
  ] =
    uuid *: text *: reconciliationStatusCodec *: timestamptz *: text.opt *:
      text.opt *: timestamptz.opt

  private def toReconciliation(
      row: (
          UUID,
          String,
          ReconciliationStatus,
          OffsetDateTime,
          Option[String],
          Option[String],
          Option[OffsetDateTime]
      )
  ): Reconciliation = {
    val (orderId, reason, status, createdAt, operatorId, note, resolvedAt) =
      row
    Reconciliation(
      OrderId.fromUuid(orderId),
      reason,
      status,
      createdAt.toInstant,
      operatorId,
      note,
      resolvedAt.map(_.toInstant)
    )
  }

  private val selectReconciliations: skunk.Query[
    (Option[String], Option[String], Option[String], Option[String]),
    (
        UUID,
        String,
        ReconciliationStatus,
        OffsetDateTime,
        Option[String],
        Option[String],
        Option[OffsetDateTime]
    )
  ] =
    sql"""
      SELECT order_id, reason, status, created_at, operator_id, note, resolved_at
      FROM order_reconciliation
      WHERE ($optionalText IS NULL OR status = $optionalText)
        AND ($optionalText IS NULL OR order_id = $optionalText::uuid)
      ORDER BY created_at DESC, order_id
    """.query(reconciliationRow)

  private val selectReconciliationByOrderId: skunk.Query[
    UUID,
    (
        UUID,
        String,
        ReconciliationStatus,
        OffsetDateTime,
        Option[String],
        Option[String],
        Option[OffsetDateTime]
    )
  ] =
    sql"""
      SELECT order_id, reason, status, created_at, operator_id, note, resolved_at
      FROM order_reconciliation
      WHERE order_id = $uuid
    """.query(reconciliationRow)

  private val resolveReconciliationQuery: skunk.Query[
    (String, Option[String], OffsetDateTime, UUID),
    (
        UUID,
        String,
        ReconciliationStatus,
        OffsetDateTime,
        Option[String],
        Option[String],
        Option[OffsetDateTime]
    )
  ] =
    sql"""
      UPDATE order_reconciliation
      SET status = 'resolved',
          operator_id = $text,
          note = $optionalText,
          resolved_at = $timestamptz
      WHERE order_id = $uuid
        AND status = 'open'
      RETURNING order_id, reason, status, created_at, operator_id, note,
                resolved_at
    """.query(reconciliationRow)

  private val insertCancellationAudit
      : skunk.Command[(UUID, String, String, String)] =
    sql"""
      INSERT INTO order_cancellation_audit (order_id, actor_type, actor_id, reason)
      VALUES ($uuid, $text, $text, $text)
      ON CONFLICT (order_id) DO NOTHING
    """.command

  private val selectCancellationAudit
      : skunk.Query[UUID, (String, String, String, Instant)] =
    sql"""
      SELECT actor_type, actor_id, reason, created_at
      FROM order_cancellation_audit
      WHERE order_id = $uuid
    """.query(text *: text *: text *: instant)

  // `trace_parent` is the active span's W3C traceparent (PERF-5.4); Debezium
  // lifts it to a `traceparent` header. NULL when written outside a span.
  private val insertOutboxEvent
      : skunk.Command[(UUID, String, String, String, Option[String])] =
    sql"""
      INSERT INTO outbox_events (id, aggregate_id, payload, correlation_id, trace_parent)
      VALUES ($uuid, $text, $text::jsonb, $text, ${text.opt})
    """.command

  /** Writes one outbox row in `session`'s transaction, stamped with the active
    * trace context (`KafkaTracing.traceParent`).
    */
  private def insertOutbox[F[_]: FlatMap](
      tracer: Tracer[F],
      session: Session[F],
      row: (UUID, String, String, String)
  ): F[Unit] =
    KafkaTracing
      .traceParent(tracer)
      .flatMap(tp =>
        session.prepare(insertOutboxEvent).flatMap(_.execute(row :* tp)).void
      )

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
      meter: Meter[F],
      tracer: Tracer[F]
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
      .evalMap(PoolMetrics.instrument(meter, "order")(_))
      .evalMap { pool =>
        meter
          .histogram[Double]("db.client.operation.duration")
          .withUnit("s")
          .withExplicitBucketBoundaries(LatencyBuckets.seconds)
          .create
          .flatMap { histogram =>
            meter
              .counter[Long]("reconciliation.flags.recorded")
              .withDescription(
                "Manual-reconciliation flags actually recorded (first-wins)."
              )
              .create
              .map { flagsCounter =>
                /** Times a Skunk query, recording a
                  * `db.client.operation.duration` measurement tagged with
                  * `db.system`/`db.operation` (OTel semantic-convention names),
                  * plus `error.type` if it fails - this is this service's only
                  * Postgres consumer, so it's instrumented directly here rather
                  * than via a new purerest combinator.
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
                  * `session.transaction.use` block as the state-change UPDATE
                  * it's meant to be atomic with (ADR 0008's whole point: Kafka
                  * delivery becomes a property of that one Postgres transaction
                  * committing).
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
                    _ <- insertOutbox(
                      tracer,
                      session,
                      (
                        outboxId,
                        OrderId.render(ref.orderId),
                        envelope.asJson.noSpaces,
                        OrderId.render(ref.orderId)
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
                        Option[Address],
                        Option[String]
                    )
                ): Order = {
                  val (
                    customerId,
                    totalCents,
                    status,
                    createdAt,
                    updatedAt,
                    address,
                    email
                  ) =
                    row
                  Order(
                    id,
                    customerId,
                    totalCents,
                    status,
                    createdAt.toInstant,
                    updatedAt.toInstant,
                    address,
                    email
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
                                    order.shippingAddress,
                                    order.guestEmail
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
                        session.transaction.use { _ =>
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
                          } yield row
                            .map(r => (toOrder(id, r), items.getOrElse(Nil)))
                        }
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
                            case (
                                  id,
                                  total,
                                  status,
                                  created,
                                  updated,
                                  address,
                                  email
                                ) =>
                              (
                                Order(
                                  OrderId.fromUuid(id),
                                  customerId,
                                  total,
                                  status,
                                  created.toInstant,
                                  updated.toInstant,
                                  address,
                                  email
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
                        row: (UUID, String, Int, OrderStatus, OffsetDateTime)
                    ): UpdatedOrderRef = {
                      val (orderId, customerId, totalCents, from, enteredAt) =
                        row
                      UpdatedOrderRef(
                        OrderId.fromUuid(orderId),
                        customerId,
                        totalCents,
                        from,
                        enteredAt.toInstant
                      )
                    }
                    timed("transition") {
                      Sync[F].realTimeInstant.flatMap { now =>
                        pool.use { session =>
                          session.transaction.use { _ =>
                            val updatedF: F[Option[UpdatedOrderRef]] =
                              allowed.toList.sortBy(_.asString) match {
                                case only :: Nil =>
                                  session
                                    .prepare(updateOrderStatusIfCurrent)
                                    .flatMap(_.option((uuid, target, only)))
                                    .map(_.map(ref))
                                case first :: second :: Nil =>
                                  session
                                    .prepare(updateOrderStatusIfAwaitingCapture)
                                    .flatMap(
                                      _.option((uuid, target, first, second))
                                    )
                                    .map(_.map(ref))
                                case unsupported =>
                                  Sync[F].raiseError(
                                    new IllegalStateException(
                                      s"Unsupported transition rule for event ${event.label}: $unsupported"
                                    )
                                  )
                              }
                            for {
                              updated <- updatedF
                              result <- updated match {
                                case Some(updatedRef) =>
                                  outboxEvent.traverse_(event =>
                                    insertOutboxRow(session, updatedRef, event)
                                  ) *>
                                    reconciliationReason
                                      .fold(Sync[F].pure(false))(reason =>
                                        session
                                          .prepare(insertReconciliation)
                                          .flatMap(
                                            _.option((uuid, reason))
                                          )
                                          .map(_.isDefined)
                                      )
                                      .flatMap { flagInserted =>
                                        OrderCancellation
                                          .auditFor(event, updatedRef, now)
                                          .traverse_(audit =>
                                            session
                                              .prepare(insertCancellationAudit)
                                              .flatMap(
                                                _.execute(
                                                  (
                                                    uuid,
                                                    audit.actorType,
                                                    audit.actorId,
                                                    audit.reason
                                                  )
                                                )
                                              )
                                          ) *>
                                          Sync[F].pure(
                                            (
                                              Right(
                                                updatedRef
                                              ): Either[
                                                TransitionRejected,
                                                UpdatedOrderRef
                                              ],
                                              flagInserted
                                            )
                                          )
                                      }
                                case None =>
                                  session
                                    .prepare(selectStatus)
                                    .flatMap(_.option(uuid))
                                    .map {
                                      case None =>
                                        (
                                          Left(TransitionRejected.UnknownOrder),
                                          false
                                        )
                                      case Some(current) =>
                                        (
                                          Left(
                                            TransitionRejected
                                              .IllegalTransition(
                                                current,
                                                target
                                              )
                                          ),
                                          false
                                        )
                                    }
                              }
                            } yield result
                          }
                        }
                      }
                    }.flatMap { case (result, flagInserted) =>
                      val recordFlag =
                        reconciliationReason.fold(Sync[F].unit)(reason =>
                          flagsCounter
                            .add(1L, List(Attribute("reason", reason)))
                            .attempt
                            .void
                        )
                      (if (flagInserted) recordFlag else Sync[F].unit)
                        .as(result)
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
                    }.flatMap {
                      case true =>
                        flagsCounter
                          .add(1L, List(Attribute("reason", reason)))
                          .attempt
                          .void
                          .as(true)
                      case false => Sync[F].pure(false)
                    }
                  }

                  def listAll(
                      status: Option[OrderStatus],
                      customerId: Option[String]
                  ): F[List[(Order, List[OrderItem])]] =
                    timed("select_all") {
                      pool.use(
                        _.execute(selectAllOrders)(
                          (status, status, customerId, customerId)
                        )
                      )
                    }.flatMap { rows =>
                      val ids = rows.map(_._1)
                      if (ids.isEmpty) Sync[F].pure(Nil)
                      else
                        timed("select_items_batch") {
                          pool.use(
                            _.execute(selectOrderItemsForOrders)(
                              ids.mkString(",")
                            )
                          )
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
                            case (
                                  id,
                                  rowCustomerId,
                                  total,
                                  rowStatus,
                                  created,
                                  updated,
                                  address,
                                  email
                                ) =>
                              (
                                Order(
                                  OrderId.fromUuid(id),
                                  rowCustomerId,
                                  total,
                                  rowStatus,
                                  created.toInstant,
                                  updated.toInstant,
                                  address,
                                  email
                                ),
                                grouped.getOrElse(id, Nil)
                              )
                          }
                        }
                    }

                  def listReconciliations(
                      status: Option[ReconciliationStatus],
                      orderId: Option[OrderId]
                  ): F[List[Reconciliation]] =
                    timed("select_reconciliations") {
                      pool.use(
                        _.execute(selectReconciliations)(
                          (
                            status.map(_.asString),
                            status.map(_.asString),
                            orderId.map(OrderId.render),
                            orderId.map(OrderId.render)
                          )
                        )
                      )
                    }.map(_.map(toReconciliation))

                  def resolveReconciliation(
                      id: OrderId,
                      operatorId: String,
                      note: Option[String],
                      resolvedAt: Instant
                  ): F[ResolveReconciliationOutcome] =
                    timed("resolve_reconciliation") {
                      pool.use { session =>
                        session.transaction.use { _ =>
                          session
                            .prepare(resolveReconciliationQuery)
                            .flatMap(
                              _.option(
                                (
                                  operatorId,
                                  note,
                                  OffsetDateTime.ofInstant(
                                    resolvedAt,
                                    ZoneOffset.UTC
                                  ),
                                  OrderId.uuid(id)
                                )
                              )
                            )
                            .flatMap {
                              case Some(row) =>
                                Sync[F].pure(
                                  ResolveReconciliationOutcome.Resolved(
                                    toReconciliation(row)
                                  )
                                )
                              case None =>
                                session
                                  .prepare(selectReconciliationByOrderId)
                                  .flatMap(_.option(OrderId.uuid(id)))
                                  .map {
                                    case Some(row) =>
                                      ResolveReconciliationOutcome
                                        .AlreadyResolved(
                                          toReconciliation(row)
                                        )
                                    case None =>
                                      ResolveReconciliationOutcome.NotFound
                                  }
                            }
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

                  def cancellationAudit(
                      id: OrderId
                  ): F[Option[CancellationAudit]] =
                    timed("select_cancellation_audit") {
                      pool
                        .use { session =>
                          session
                            .prepare(selectCancellationAudit)
                            .flatMap(_.option(OrderId.uuid(id)))
                        }
                        .map(_.map {
                          case (actorType, actorId, reason, createdAt) =>
                            CancellationAudit(
                              actorType,
                              actorId,
                              reason,
                              createdAt
                            )
                        })
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
}
