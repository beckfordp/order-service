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

class PaymentEventConsumerSuite
    extends CatsEffectSuite
    with TestContainerForAll {

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

  private val oneItem =
    List(NewOrderItem("sku-1", "Widget", 999, 2))

  test(
    "a synthetic payment.settled event flips a Reserved order's status to Confirmed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        store <- OrderStore.inMemory[IO]
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Reserved)
        event = PaymentSettledEvent(
          order.id,
          "payment-1",
          order.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.settledTopic,
          order.id,
          event.asJson.noSpaces
        )
        finalStatus <- IO.race(
          PaymentEventConsumer
            .run[IO](
              config,
              store,
              NoOpLogger[IO],
              OrderEventPublisher.noOp[IO]
            )
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.Confirmed)
        )
      } yield assertEquals(finalStatus, Right(OrderStatus.Confirmed))
    }
  }

  test(
    "a synthetic payment.failed event flips a Reserved order's status to PaymentFailed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        store <- OrderStore.inMemory[IO]
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Reserved)
        event = PaymentFailedEvent(
          order.id,
          "payment-1",
          order.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.failedTopic,
          order.id,
          event.asJson.noSpaces
        )
        finalStatus <- IO.race(
          PaymentEventConsumer
            .run[IO](
              config,
              store,
              NoOpLogger[IO],
              OrderEventPublisher.noOp[IO]
            )
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.PaymentFailed)
        )
      } yield assertEquals(finalStatus, Right(OrderStatus.PaymentFailed))
    }
  }

  test(
    "an event for an order that's not Reserved is a no-op and doesn't block later events"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        store <- OrderStore.inMemory[IO]
        (stalePendingOrder, _) <- store.create("cust-123", oneItem)
        (liveOrder, _) <- store.create("cust-456", oneItem)
        _ <- store.update(liveOrder.id, OrderStatus.Reserved)
        staleEvent = PaymentSettledEvent(
          stalePendingOrder.id,
          "payment-stale",
          stalePendingOrder.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        liveEvent = PaymentSettledEvent(
          liveOrder.id,
          "payment-live",
          liveOrder.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.settledTopic,
          stalePendingOrder.id,
          staleEvent.asJson.noSpaces
        )
        _ <- produce(
          config,
          PaymentEventConsumer.settledTopic,
          liveOrder.id,
          liveEvent.asJson.noSpaces
        )
        finalStatus <- IO.race(
          PaymentEventConsumer
            .run[IO](
              config,
              store,
              NoOpLogger[IO],
              OrderEventPublisher.noOp[IO]
            )
            .compile
            .drain,
          pollForStatus(store, liveOrder.id, OrderStatus.Confirmed)
        )
        stalePendingStatus <- statusOf(store, stalePendingOrder.id)
      } yield {
        assertEquals(finalStatus, Right(OrderStatus.Confirmed))
        assertEquals(stalePendingStatus, OrderStatus.Pending)
      }
    }
  }

  test(
    "an event for an unknown orderId is a no-op and doesn't block later events"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        store <- OrderStore.inMemory[IO]
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Reserved)
        unknownEvent = PaymentSettledEvent(
          "unknown-order-id",
          "payment-unknown",
          999,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        realEvent = PaymentSettledEvent(
          order.id,
          "payment-1",
          order.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.settledTopic,
          "unknown-order-id",
          unknownEvent.asJson.noSpaces
        )
        _ <- produce(
          config,
          PaymentEventConsumer.settledTopic,
          order.id,
          realEvent.asJson.noSpaces
        )
        finalStatus <- IO.race(
          PaymentEventConsumer
            .run[IO](
              config,
              store,
              NoOpLogger[IO],
              OrderEventPublisher.noOp[IO]
            )
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.Confirmed)
        )
      } yield assertEquals(finalStatus, Right(OrderStatus.Confirmed))
    }
  }

  test(
    "a malformed payment.settled event is a no-op and doesn't block later events"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        store <- OrderStore.inMemory[IO]
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Reserved)
        realEvent = PaymentSettledEvent(
          order.id,
          "payment-1",
          order.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.settledTopic,
          "bad-key",
          "{not valid json"
        )
        _ <- produce(
          config,
          PaymentEventConsumer.settledTopic,
          order.id,
          realEvent.asJson.noSpaces
        )
        finalStatus <- IO.race(
          PaymentEventConsumer
            .run[IO](
              config,
              store,
              NoOpLogger[IO],
              OrderEventPublisher.noOp[IO]
            )
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.Confirmed)
        )
      } yield assertEquals(finalStatus, Right(OrderStatus.Confirmed))
    }
  }

  test(
    "payment.settled and payment.failed route to their own distinct target statuses, not the other's"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        store <- OrderStore.inMemory[IO]
        (settledOrder, _) <- store.create("cust-123", oneItem)
        _ <- store.update(settledOrder.id, OrderStatus.Reserved)
        (failedOrder, _) <- store.create("cust-456", oneItem)
        _ <- store.update(failedOrder.id, OrderStatus.Reserved)
        settledEvent = PaymentSettledEvent(
          settledOrder.id,
          "payment-settled",
          settledOrder.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        failedEvent = PaymentFailedEvent(
          failedOrder.id,
          "payment-failed",
          failedOrder.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.settledTopic,
          settledOrder.id,
          settledEvent.asJson.noSpaces
        )
        _ <- produce(
          config,
          PaymentEventConsumer.failedTopic,
          failedOrder.id,
          failedEvent.asJson.noSpaces
        )
        result <- IO.race(
          PaymentEventConsumer
            .run[IO](
              config,
              store,
              NoOpLogger[IO],
              OrderEventPublisher.noOp[IO]
            )
            .compile
            .drain,
          for {
            s <- pollForStatus(store, settledOrder.id, OrderStatus.Confirmed)
            f <- pollForStatus(store, failedOrder.id, OrderStatus.PaymentFailed)
          } yield (s, f)
        )
      } yield assertEquals(
        result,
        Right((OrderStatus.Confirmed, OrderStatus.PaymentFailed))
      )
    }
  }

  test(
    "a synthetic payment.settled event also publishes order.status-changed with status=confirmed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      OrderEventPublisher.resource[IO](config, NoOpLogger[IO]).use {
        publisher =>
          for {
            store <- OrderStore.inMemory[IO]
            (order, _) <- store.create("cust-123", oneItem)
            _ <- store.update(order.id, OrderStatus.Reserved)
            event = PaymentSettledEvent(
              order.id,
              "payment-1",
              order.totalCents,
              Instant.parse("2026-01-01T00:00:00Z")
            )
            _ <- produce(
              config,
              PaymentEventConsumer.settledTopic,
              order.id,
              event.asJson.noSpaces
            )
            published <- IO.race(
              PaymentEventConsumer
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
                  assertEquals(event.status, "confirmed")
              }
          }
      }
    }
  }

  test(
    "a synthetic payment.failed event also publishes order.status-changed with status=payment_failed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      OrderEventPublisher.resource[IO](config, NoOpLogger[IO]).use {
        publisher =>
          for {
            store <- OrderStore.inMemory[IO]
            (order, _) <- store.create("cust-123", oneItem)
            _ <- store.update(order.id, OrderStatus.Reserved)
            event = PaymentFailedEvent(
              order.id,
              "payment-1",
              order.totalCents,
              Instant.parse("2026-01-01T00:00:00Z")
            )
            _ <- produce(
              config,
              PaymentEventConsumer.failedTopic,
              order.id,
              event.asJson.noSpaces
            )
            published <- IO.race(
              PaymentEventConsumer
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
                  assertEquals(event.status, "payment_failed")
              }
          }
      }
    }
  }
}
