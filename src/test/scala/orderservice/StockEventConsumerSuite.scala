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

  /** Scans from the beginning for the first message matching `predicate`,
    * instead of blindly taking the first message ever - tests sharing this
    * suite's one Testcontainers Kafka instance can run concurrently, each with
    * a real (non-noOp) publisher writing to the same topic, so "the first
    * message on the topic" isn't reliably "this test's own message."
    */
  private def consumeMatching(
      config: KafkaConfig,
      topic: String,
      predicate: String => Boolean
  ): IO[String] = {
    val consumerSettings =
      ConsumerSettings[IO, String, String]
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(s"test-${java.util.UUID.randomUUID()}")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)

    KafkaConsumer.resource(consumerSettings).use { consumer =>
      for {
        _ <- consumer.subscribeTo(topic)
        record <- consumer.stream
          .map(_.record.value)
          .filter(predicate)
          .take(1)
          .compile
          .lastOrError
          .timeout(15.seconds)
      } yield record
    }
  }

  /** Returns true if no message matching `predicate` arrives on the topic
    * within the window - used to assert a negative (nothing published for *this
    * test's* order), tolerant of other tests having already published unrelated
    * messages to the same topic in this shared container.
    */
  private def noMatchingMessageWithin(
      config: KafkaConfig,
      topic: String,
      predicate: String => Boolean,
      window: FiniteDuration
  ): IO[Boolean] = {
    val consumerSettings =
      ConsumerSettings[IO, String, String]
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(s"test-${java.util.UUID.randomUUID()}")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)

    KafkaConsumer.resource(consumerSettings).use { consumer =>
      for {
        _ <- consumer.subscribeTo(topic)
        records <- consumer.stream
          .map(_.record.value)
          .interruptAfter(window)
          .compile
          .toList
      } yield !records.exists(predicate)
    }
  }

  /** Matches an envelope on `order-events` for the given orderId *and*
    * eventType - needed now that `OrderReserved`/`OrderStatusChanged` share
    * one topic, so a plain `_.contains(orderId)` would match either.
    */
  private def forOrderAndType(orderId: String, eventType: String)(
      raw: String
  ): Boolean =
    decode[EventEnvelope](raw).exists(envelope =>
      envelope.correlationId == orderId && envelope.eventType == eventType
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
            .run[IO](
              config,
              store,
              NoOpLogger[IO],
              OrderEventPublisher.noOp[IO]
            )
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
            .run[IO](
              config,
              store,
              NoOpLogger[IO],
              OrderEventPublisher.noOp[IO]
            )
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
            .run[IO](
              config,
              store,
              NoOpLogger[IO],
              OrderEventPublisher.noOp[IO]
            )
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
            .run[IO](
              config,
              store,
              NoOpLogger[IO],
              OrderEventPublisher.noOp[IO]
            )
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
            .run[IO](
              config,
              store,
              NoOpLogger[IO],
              OrderEventPublisher.noOp[IO]
            )
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
    "a synthetic StockReserved envelope also publishes OrderReserved with the order's details"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      OrderEventPublisher.resource[IO](config, NoOpLogger[IO]).use {
        publisher =>
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
            published <- IO.race(
              StockEventConsumer
                .run[IO](config, store, NoOpLogger[IO], publisher)
                .compile
                .drain,
              consumeMatching(
                config,
                OrderEventPublisher.topic,
                forOrderAndType(order.id, "OrderReserved")
              )
            )
          } yield published match {
            case Left(()) =>
              fail("consumer finished before OrderReserved was published")
            case Right(raw) =>
              val envelope = decode[EventEnvelope](raw).toOption.get
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
  }

  test(
    "a synthetic StockReservationFailed envelope never publishes OrderReserved"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      OrderEventPublisher.resource[IO](config, NoOpLogger[IO]).use {
        publisher =>
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
            result <- IO.race(
              StockEventConsumer
                .run[IO](config, store, NoOpLogger[IO], publisher)
                .compile
                .drain,
              pollForStatus(store, order.id, OrderStatus.ReservationFailed) *>
                noMatchingMessageWithin(
                  config,
                  OrderEventPublisher.topic,
                  forOrderAndType(order.id, "OrderReserved"),
                  3.seconds
                )
            )
          } yield assertEquals(result, Right(true))
      }
    }
  }

  test(
    "a synthetic StockReservationFailed envelope also publishes OrderStatusChanged with reservation_failed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      OrderEventPublisher.resource[IO](config, NoOpLogger[IO]).use {
        publisher =>
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
            published <- IO.race(
              StockEventConsumer
                .run[IO](config, store, NoOpLogger[IO], publisher)
                .compile
                .drain,
              consumeMatching(
                config,
                OrderEventPublisher.topic,
                forOrderAndType(order.id, "OrderStatusChanged")
              )
            )
          } yield published match {
            case Left(()) =>
              fail(
                "consumer finished before OrderStatusChanged was published"
              )
            case Right(raw) =>
              val envelope = decode[EventEnvelope](raw).toOption.get
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
  }

  test(
    "a synthetic StockReserved envelope never publishes OrderStatusChanged"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      OrderEventPublisher.resource[IO](config, NoOpLogger[IO]).use {
        publisher =>
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
            result <- IO.race(
              StockEventConsumer
                .run[IO](config, store, NoOpLogger[IO], publisher)
                .compile
                .drain,
              pollForStatus(store, order.id, OrderStatus.Reserved) *>
                noMatchingMessageWithin(
                  config,
                  OrderEventPublisher.topic,
                  forOrderAndType(order.id, "OrderStatusChanged"),
                  3.seconds
                )
            )
          } yield assertEquals(result, Right(true))
      }
    }
  }
}
