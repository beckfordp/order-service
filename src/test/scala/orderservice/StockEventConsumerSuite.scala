package orderservice

import cats.effect.IO
import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import fs2.kafka._
import io.circe.Json
import io.circe.parser.decode
import io.circe.syntax._
import munit.CatsEffectSuite
import org.typelevel.log4cats.noop.NoOpLogger

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration._

class StockEventConsumerSuite extends CatsEffectSuite with TestContainerForAll {

  override val containerDef: KafkaContainer.Def = KafkaContainer.Def()

  private def configFor(kafka: KafkaContainer): KafkaConfig =
    KafkaConfig(bootstrapServers = kafka.bootstrapServers)

  private def produce(
      config: KafkaConfig,
      topic: String,
      key: String,
      json: String
  ): IO[Unit] = {
    val producerSettings =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(config.bootstrapServers)
    KafkaProducer
      .resource(producerSettings)
      .use(_.produceOne_(ProducerRecord(topic, key, json)).flatten.void)
  }

  /** Sends a record with a literal `null` value — a plain `String` value
    * serializer accepts `null` at the JVM level (confirmed: this is exactly
    * how TD-2.1 found payment-service's identical crash risk, "a producer
    * that sends no key/value" — not reachable via a normal JSON payload,
    * only by bypassing the usual encode-a-case-class path). TD-2.2's own
    * null-safety fix (`Option[String]` deserializer) is what should make a
    * record like this an ignorable no-op instead of a fiber-killing crash.
    */
  private def produceNullValue(
      config: KafkaConfig,
      topic: String,
      key: String
  ): IO[Unit] = {
    // A plain String serializer throws on `null` at serialize time (before
    // the record ever reaches the broker) - Serializer.option is the
    // producer-side counterpart to the consumer's Deserializer.option,
    // letting `None` actually become a null-valued record on the wire.
    val producerSettings =
      ProducerSettings[IO, String, Option[String]](
        Serializer[IO, String],
        Serializer.option(Serializer[IO, String])
      ).withBootstrapServers(config.bootstrapServers)
    KafkaProducer
      .resource(producerSettings)
      .use(_.produceOne_(ProducerRecord(topic, key, None)).flatten.void)
  }

  private def envelopeJson(
      eventType: String,
      correlationId: String,
      payload: Json
  ): String =
    EventEnvelope(
      eventId = UUID.randomUUID().toString,
      eventType = eventType,
      timestamp = Instant.parse("2026-01-01T00:00:00Z"),
      source = "inventory-service",
      correlationId = correlationId,
      payload = payload
    ).asJson.noSpaces

  private def pollUntil[A](check: IO[Option[A]]): IO[A] =
    check.flatMap {
      case Some(a) => IO.pure(a)
      case None    => IO.sleep(200.millis) *> pollUntil(check)
    }

  private def statusOf(
      store: OrderStore[IO],
      orderId: String
  ): IO[OrderStatus] =
    store.get(orderId).map(_.map(_._1.status).get)

  private def pollForStatus(
      store: OrderStore[IO],
      orderId: String,
      expected: OrderStatus
  ): IO[OrderStatus] =
    pollUntil(
      statusOf(store, orderId).map(status =>
        Option.when(status == expected)(status)
      )
    ).timeout(15.seconds)

  /** Polls an `inMemoryWithOutbox` ref for an outbox row matching `orderId`
    * and `eventType` - the outbox write (ADR 0008, TD-3.2) replaces the old
    * real-Kafka-publish-and-reconsume round trip these tests used to need;
    * the write lands in-process, synchronously with the consumer's own
    * store call, so polling in-memory state is both simpler and faster than
    * a second Kafka consumer ever was.
    */
  private def pollForOutboxEvent(
      outboxRef: cats.effect.Ref[IO, List[(String, EventEnvelope)]],
      orderId: String,
      eventType: String
  ): IO[EventEnvelope] =
    pollUntil(
      outboxRef.get.map(
        _.collectFirst {
          case (id, envelope)
              if id == orderId && envelope.eventType == eventType =>
            envelope
        }
      )
    ).timeout(15.seconds)

  private def outboxHasNo(
      outboxRef: cats.effect.Ref[IO, List[(String, EventEnvelope)]],
      orderId: String,
      eventType: String
  ): IO[Boolean] =
    outboxRef.get.map(
      !_.exists { case (id, envelope) =>
        id == orderId && envelope.eventType == eventType
      }
    )

  private val oneItem =
    List(NewOrderItem("sku-1", "Widget", 999, 2))

  test(
    "a synthetic StockReserved envelope flips a Pending order's status to Reserved"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        store <- OrderStore.inMemory[IO]
        (order, items) <- store.create("cust-123", oneItem)
        event = StockReservedEvent(
          items.head.id,
          "sku-1",
          2,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          StockEventConsumer.topic,
          "sku-1",
          envelopeJson("StockReserved", items.head.id, event.asJson)
        )
        finalStatus <- IO.race(
          StockEventConsumer
            .run[IO](config, store, NoOpLogger[IO])
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.Reserved)
        )
      } yield assertEquals(finalStatus, Right(OrderStatus.Reserved))
    }
  }

  test(
    "a synthetic StockReservationFailed envelope flips a Pending order's status to ReservationFailed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        store <- OrderStore.inMemory[IO]
        (order, items) <- store.create("cust-123", oneItem)
        event = StockReservationFailedEvent(
          items.head.id,
          "sku-1",
          2,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          StockEventConsumer.topic,
          "sku-1",
          envelopeJson(
            "StockReservationFailed",
            items.head.id,
            event.asJson
          )
        )
        finalStatus <- IO.race(
          StockEventConsumer
            .run[IO](config, store, NoOpLogger[IO])
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.ReservationFailed)
        )
      } yield assertEquals(finalStatus, Right(OrderStatus.ReservationFailed))
    }
  }

  test(
    "an envelope with a different eventType on inventory-events is ignored, not treated as malformed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        store <- OrderStore.inMemory[IO]
        (order, items) <- store.create("cust-123", oneItem)
        otherPayload = Json.obj(
          "sku" -> Json.fromString("sku-other"),
          "quantityAvailable" -> Json.fromInt(5),
          "quantityReserved" -> Json.fromInt(0)
        )
        realEvent = StockReservedEvent(
          items.head.id,
          "sku-1",
          2,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          StockEventConsumer.topic,
          "sku-other",
          envelopeJson("InventoryAdjusted", "irrelevant", otherPayload)
        )
        _ <- produce(
          config,
          StockEventConsumer.topic,
          "sku-1",
          envelopeJson("StockReserved", items.head.id, realEvent.asJson)
        )
        finalStatus <- IO.race(
          StockEventConsumer
            .run[IO](config, store, NoOpLogger[IO])
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.Reserved)
        )
      } yield assertEquals(finalStatus, Right(OrderStatus.Reserved))
    }
  }

  test(
    "an event for an unknown orderItemId is a no-op and doesn't block later events"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        store <- OrderStore.inMemory[IO]
        (order, items) <- store.create("cust-123", oneItem)
        unknownEvent = StockReservedEvent(
          "unknown-item-id",
          "sku-unknown",
          1,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        realEvent = StockReservedEvent(
          items.head.id,
          "sku-1",
          2,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          StockEventConsumer.topic,
          "sku-unknown",
          envelopeJson(
            "StockReserved",
            "unknown-item-id",
            unknownEvent.asJson
          )
        )
        _ <- produce(
          config,
          StockEventConsumer.topic,
          "sku-1",
          envelopeJson("StockReserved", items.head.id, realEvent.asJson)
        )
        finalStatus <- IO.race(
          StockEventConsumer
            .run[IO](config, store, NoOpLogger[IO])
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.Reserved)
        )
      } yield assertEquals(finalStatus, Right(OrderStatus.Reserved))
    }
  }

  test(
    "an event for an already-resolved order is a no-op and doesn't block later events"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        store <- OrderStore.inMemory[IO]
        (resolvedOrder, resolvedItems) <- store.create("cust-123", oneItem)
        _ <- store.update(resolvedOrder.id, OrderStatus.Reserved)
        (otherOrder, otherItems) <- store.create("cust-456", oneItem)
        staleEvent = StockReservationFailedEvent(
          resolvedItems.head.id,
          "sku-1",
          2,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        liveEvent = StockReservedEvent(
          otherItems.head.id,
          "sku-1",
          2,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          StockEventConsumer.topic,
          "sku-1",
          envelopeJson(
            "StockReservationFailed",
            resolvedItems.head.id,
            staleEvent.asJson
          )
        )
        _ <- produce(
          config,
          StockEventConsumer.topic,
          "sku-1",
          envelopeJson("StockReserved", otherItems.head.id, liveEvent.asJson)
        )
        finalStatus <- IO.race(
          StockEventConsumer
            .run[IO](config, store, NoOpLogger[IO])
            .compile
            .drain,
          pollForStatus(store, otherOrder.id, OrderStatus.Reserved)
        )
        resolvedOrderStatus <- statusOf(store, resolvedOrder.id)
      } yield {
        assertEquals(finalStatus, Right(OrderStatus.Reserved))
        assertEquals(resolvedOrderStatus, OrderStatus.Reserved)
      }
    }
  }

  test(
    "a synthetic StockReserved envelope also writes an OrderReserved outbox row with the order's details"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        storeAndOutbox <- OrderStore.inMemoryWithOutbox[IO]
        (store, outboxRef) = storeAndOutbox
        (order, items) <- store.create("cust-123", oneItem)
        event = StockReservedEvent(
          items.head.id,
          "sku-1",
          2,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          StockEventConsumer.topic,
          "sku-1",
          envelopeJson("StockReserved", items.head.id, event.asJson)
        )
        outboxed <- IO.race(
          StockEventConsumer
            .run[IO](config, store, NoOpLogger[IO])
            .compile
            .drain,
          pollForOutboxEvent(outboxRef, order.id, "OrderReserved")
        )
      } yield outboxed match {
        case Left(()) =>
          fail("consumer finished before the OrderReserved outbox row appeared")
        case Right(envelope) =>
          envelope.payload.as[OrderReservedEvent] match {
            case Left(error)  => fail(s"failed to decode: $error")
            case Right(event) =>
              assertEquals(event.orderId, order.id)
              assertEquals(event.customerId, "cust-123")
              assertEquals(event.totalCents, order.totalCents)
          }
      }
    }
  }

  test(
    "a synthetic StockReservationFailed envelope never writes an OrderReserved outbox row"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        storeAndOutbox <- OrderStore.inMemoryWithOutbox[IO]
        (store, outboxRef) = storeAndOutbox
        (order, items) <- store.create("cust-123", oneItem)
        event = StockReservationFailedEvent(
          items.head.id,
          "sku-1",
          2,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          StockEventConsumer.topic,
          "sku-1",
          envelopeJson("StockReservationFailed", items.head.id, event.asJson)
        )
        result <- IO.race(
          StockEventConsumer
            .run[IO](config, store, NoOpLogger[IO])
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.ReservationFailed)
        )
        noOutboxRow <- outboxHasNo(outboxRef, order.id, "OrderReserved")
      } yield {
        assertEquals(result, Right(OrderStatus.ReservationFailed))
        assert(noOutboxRow)
      }
    }
  }

  test(
    "a synthetic StockReservationFailed envelope also writes an OrderStatusChanged outbox row with reservation_failed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        storeAndOutbox <- OrderStore.inMemoryWithOutbox[IO]
        (store, outboxRef) = storeAndOutbox
        (order, items) <- store.create("cust-123", oneItem)
        event = StockReservationFailedEvent(
          items.head.id,
          "sku-1",
          2,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          StockEventConsumer.topic,
          "sku-1",
          envelopeJson("StockReservationFailed", items.head.id, event.asJson)
        )
        outboxed <- IO.race(
          StockEventConsumer
            .run[IO](config, store, NoOpLogger[IO])
            .compile
            .drain,
          pollForOutboxEvent(outboxRef, order.id, "OrderStatusChanged")
        )
      } yield outboxed match {
        case Left(()) =>
          fail(
            "consumer finished before the OrderStatusChanged outbox row appeared"
          )
        case Right(envelope) =>
          envelope.payload.as[OrderStatusChangedEvent] match {
            case Left(error)  => fail(s"failed to decode: $error")
            case Right(event) =>
              assertEquals(event.orderId, order.id)
              assertEquals(event.customerId, "cust-123")
              assertEquals(event.status, "reservation_failed")
          }
      }
    }
  }

  test(
    "a synthetic StockReserved envelope never writes an OrderStatusChanged outbox row"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        storeAndOutbox <- OrderStore.inMemoryWithOutbox[IO]
        (store, outboxRef) = storeAndOutbox
        (order, items) <- store.create("cust-123", oneItem)
        event = StockReservedEvent(
          items.head.id,
          "sku-1",
          2,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          StockEventConsumer.topic,
          "sku-1",
          envelopeJson("StockReserved", items.head.id, event.asJson)
        )
        result <- IO.race(
          StockEventConsumer
            .run[IO](config, store, NoOpLogger[IO])
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.Reserved)
        )
        noOutboxRow <- outboxHasNo(outboxRef, order.id, "OrderStatusChanged")
      } yield {
        assertEquals(result, Right(OrderStatus.Reserved))
        assert(noOutboxRow)
      }
    }
  }

  test(
    "a record with a null value doesn't crash the consumer, and a later valid event still updates order status (TD-2.2)"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        store <- OrderStore.inMemory[IO]
        (order, items) <- store.create("cust-123", oneItem)
        event = StockReservedEvent(
          items.head.id,
          "sku-1",
          2,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produceNullValue(config, StockEventConsumer.topic, "sku-1")
        _ <- produce(
          config,
          StockEventConsumer.topic,
          "sku-1",
          envelopeJson("StockReserved", items.head.id, event.asJson)
        )
        finalStatus <- IO.race(
          StockEventConsumer
            .run[IO](config, store, NoOpLogger[IO])
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.Reserved)
        )
      } yield assertEquals(finalStatus, Right(OrderStatus.Reserved))
    }
  }
}
