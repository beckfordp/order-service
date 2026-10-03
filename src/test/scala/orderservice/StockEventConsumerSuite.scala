package orderservice

import cats.effect.IO
import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import fs2.kafka._
import io.circe.parser.decode
import io.circe.syntax._
import munit.CatsEffectSuite
import org.typelevel.log4cats.noop.NoOpLogger

import java.time.Instant
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

  private val oneItem =
    List(NewOrderItem("sku-1", "Widget", 999, 2))

  test(
    "a synthetic stock-reserved event flips a Pending order's status to Reserved"
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
          StockEventConsumer.reservedTopic,
          "sku-1",
          event.asJson.noSpaces
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
    "a synthetic stock-reservation-failed event flips a Pending order's status to ReservationFailed"
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
          StockEventConsumer.reservationFailedTopic,
          "sku-1",
          event.asJson.noSpaces
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
          StockEventConsumer.reservedTopic,
          "sku-unknown",
          unknownEvent.asJson.noSpaces
        )
        _ <- produce(
          config,
          StockEventConsumer.reservedTopic,
          "sku-1",
          realEvent.asJson.noSpaces
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
          StockEventConsumer.reservationFailedTopic,
          "sku-1",
          staleEvent.asJson.noSpaces
        )
        _ <- produce(
          config,
          StockEventConsumer.reservedTopic,
          "sku-1",
          liveEvent.asJson.noSpaces
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
    "a synthetic stock-reserved event also publishes order.reserved with the order's details"
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
              StockEventConsumer.reservedTopic,
              "sku-1",
              event.asJson.noSpaces
            )
            published <- IO.race(
              StockEventConsumer
                .run[IO](config, store, NoOpLogger[IO], publisher)
                .compile
                .drain,
              consumeMatching(
                config,
                OrderEventPublisher.reservedTopic,
                _.contains(order.id)
              )
            )
          } yield published match {
            case Left(()) =>
              fail("consumer finished before order.reserved was published")
            case Right(raw) =>
              decode[OrderReservedEvent](raw) match {
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
    "a synthetic stock-reservation-failed event never publishes order.reserved"
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
              StockEventConsumer.reservationFailedTopic,
              "sku-1",
              event.asJson.noSpaces
            )
            result <- IO.race(
              StockEventConsumer
                .run[IO](config, store, NoOpLogger[IO], publisher)
                .compile
                .drain,
              pollForStatus(store, order.id, OrderStatus.ReservationFailed) *>
                noMatchingMessageWithin(
                  config,
                  OrderEventPublisher.reservedTopic,
                  _.contains(order.id),
                  3.seconds
                )
            )
          } yield assertEquals(result, Right(true))
      }
    }
  }

  test(
    "a synthetic stock-reservation-failed event also publishes order.status-changed with reservation_failed"
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
              StockEventConsumer.reservationFailedTopic,
              "sku-1",
              event.asJson.noSpaces
            )
            published <- IO.race(
              StockEventConsumer
                .run[IO](config, store, NoOpLogger[IO], publisher)
                .compile
                .drain,
              consumeMatching(
                config,
                OrderEventPublisher.statusChangedTopic,
                _.contains(order.id)
              )
            )
          } yield published match {
            case Left(()) =>
              fail(
                "consumer finished before order.status-changed was published"
              )
            case Right(raw) =>
              decode[OrderStatusChangedEvent](raw) match {
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
    "a synthetic stock-reserved event never publishes order.status-changed"
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
              StockEventConsumer.reservedTopic,
              "sku-1",
              event.asJson.noSpaces
            )
            result <- IO.race(
              StockEventConsumer
                .run[IO](config, store, NoOpLogger[IO], publisher)
                .compile
                .drain,
              pollForStatus(store, order.id, OrderStatus.Reserved) *>
                noMatchingMessageWithin(
                  config,
                  OrderEventPublisher.statusChangedTopic,
                  _.contains(order.id),
                  3.seconds
                )
            )
          } yield assertEquals(result, Right(true))
      }
    }
  }
}
