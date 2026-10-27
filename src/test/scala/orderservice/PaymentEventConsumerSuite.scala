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
import org.typelevel.otel4s.metrics.Meter
import purekafka.consumer.DeadLetter
import purekafka.resilience.{
  CircuitBreakerConfig,
  ResilienceConfig,
  RetryConfig
}

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration._

class PaymentEventConsumerSuite
    extends CatsEffectSuite
    with TestContainerForAll {

  /** A `DeadLetter` whose `guard` never actually triggers in these tests -
    * every `handleCaptured`/`handleExpired`/`handleFailed` call here succeeds
    * on its first attempt, so this is only ever a transparent pass-through.
    * `KafkaConsumersSuite` is where DLQ routing itself (the failure path) is
    * actually exercised.
    */
  private def noOpDeadLetter: IO[DeadLetter[IO, String, String]] = {
    import org.typelevel.otel4s.metrics.Meter.Implicits.noop
    DeadLetter.fromPublish[IO, String, String](
      RetryConfig(maxRetries = 0, baseDelay = 1.milli),
      ResilienceConfig(
        retry = RetryConfig(maxRetries = 0, baseDelay = 1.milli),
        circuitBreaker =
          CircuitBreakerConfig(failureThreshold = 100, resetTimeout = 1.second)
      ),
      (_, _, _) => IO.unit
    )(NoOpLogger[IO])(summon[Meter[IO]])
  }

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

  /** Sends a record with a literal `null` value - a plain `String` value
    * serializer throws on `null` at serialize time, so `Serializer.option` (the
    * producer-side counterpart to the consumer's `Deserializer.option`) is
    * needed to actually get a null-valued record onto the wire. TD-2.2's own
    * null-safety fix (`Option[String]` deserializer) is what should make a
    * record like this an ignorable no-op instead of a fiber-killing crash.
    */
  private def produceNullValue(
      config: KafkaConfig,
      topic: String,
      key: String
  ): IO[Unit] = {
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
      source = "payment-service",
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

  /** Polls an `inMemoryWithOutbox` ref for an outbox row matching `orderId` and
    * `eventType` - the outbox write (ADR 0008, TD-3.4) replaces the old
    * real-Kafka-publish-and-reconsume round trip these tests used to need.
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

  private val oneItem =
    List(NewOrderItem("sku-1", "Widget", 999, 2))

  test(
    "a synthetic PaymentCaptured envelope flips a Reserved order's status to Confirmed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- OrderStore.inMemory[IO]
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Reserved)
        event = PaymentCapturedEvent(
          order.id,
          order.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          order.id,
          envelopeJson("PaymentCaptured", order.id, event.asJson)
        )
        finalStatus <- IO.race(
          PaymentEventConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.Confirmed)
        )
      } yield assertEquals(finalStatus, Right(OrderStatus.Confirmed))
    }
  }

  test(
    "a synthetic PaymentAuthorizationExpired envelope flips a Reserved order's status to Cancelled"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- OrderStore.inMemory[IO]
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Reserved)
        event = PaymentAuthorizationExpiredEvent(
          order.id,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          order.id,
          envelopeJson("PaymentAuthorizationExpired", order.id, event.asJson)
        )
        finalStatus <- IO.race(
          PaymentEventConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.Cancelled)
        )
      } yield assertEquals(finalStatus, Right(OrderStatus.Cancelled))
    }
  }

  test(
    "a synthetic PaymentFailed envelope flips a Reserved order's status to PaymentFailed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- OrderStore.inMemory[IO]
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Reserved)
        event = PaymentFailedEvent(
          order.id,
          "payment-1",
          order.totalCents,
          "capture_failed",
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          order.id,
          envelopeJson("PaymentFailed", order.id, event.asJson)
        )
        finalStatus <- IO.race(
          PaymentEventConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.PaymentFailed)
        )
      } yield assertEquals(finalStatus, Right(OrderStatus.PaymentFailed))
    }
  }

  test(
    "an envelope with a different eventType on payment-events is ignored, not treated as malformed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- OrderStore.inMemory[IO]
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Reserved)
        realEvent = PaymentCapturedEvent(
          order.id,
          order.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        otherPayload = Json.obj(
          "paymentId" -> Json.fromString("payment-irrelevant"),
          "reason" -> Json.fromString("refunded")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          "payment-irrelevant",
          envelopeJson("PaymentRefunded", "irrelevant", otherPayload)
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          order.id,
          envelopeJson("PaymentCaptured", order.id, realEvent.asJson)
        )
        finalStatus <- IO.race(
          PaymentEventConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.Confirmed)
        )
      } yield assertEquals(finalStatus, Right(OrderStatus.Confirmed))
    }
  }

  test(
    "an event for an order that's not Reserved is a no-op and doesn't block later events"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- OrderStore.inMemory[IO]
        (stalePendingOrder, _) <- store.create("cust-123", oneItem)
        (liveOrder, _) <- store.create("cust-456", oneItem)
        _ <- store.update(liveOrder.id, OrderStatus.Reserved)
        staleEvent = PaymentCapturedEvent(
          stalePendingOrder.id,
          stalePendingOrder.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        liveEvent = PaymentCapturedEvent(
          liveOrder.id,
          liveOrder.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          stalePendingOrder.id,
          envelopeJson(
            "PaymentCaptured",
            stalePendingOrder.id,
            staleEvent.asJson
          )
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          liveOrder.id,
          envelopeJson("PaymentCaptured", liveOrder.id, liveEvent.asJson)
        )
        finalStatus <- IO.race(
          PaymentEventConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
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
        deadLetter <- noOpDeadLetter
        store <- OrderStore.inMemory[IO]
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Reserved)
        unknownEvent = PaymentCapturedEvent(
          "unknown-order-id",
          999,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        realEvent = PaymentCapturedEvent(
          order.id,
          order.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          "unknown-order-id",
          envelopeJson(
            "PaymentCaptured",
            "unknown-order-id",
            unknownEvent.asJson
          )
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          order.id,
          envelopeJson("PaymentCaptured", order.id, realEvent.asJson)
        )
        finalStatus <- IO.race(
          PaymentEventConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.Confirmed)
        )
      } yield assertEquals(finalStatus, Right(OrderStatus.Confirmed))
    }
  }

  test(
    "a malformed envelope is a no-op and doesn't block later events"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- OrderStore.inMemory[IO]
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Reserved)
        realEvent = PaymentCapturedEvent(
          order.id,
          order.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          "bad-key",
          "{not valid json"
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          order.id,
          envelopeJson("PaymentCaptured", order.id, realEvent.asJson)
        )
        finalStatus <- IO.race(
          PaymentEventConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.Confirmed)
        )
      } yield assertEquals(finalStatus, Right(OrderStatus.Confirmed))
    }
  }

  test(
    "PaymentCaptured and PaymentFailed route to their own distinct target statuses, not the other's"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- OrderStore.inMemory[IO]
        (settledOrder, _) <- store.create("cust-123", oneItem)
        _ <- store.update(settledOrder.id, OrderStatus.Reserved)
        (failedOrder, _) <- store.create("cust-456", oneItem)
        _ <- store.update(failedOrder.id, OrderStatus.Reserved)
        settledEvent = PaymentCapturedEvent(
          settledOrder.id,
          settledOrder.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        failedEvent = PaymentFailedEvent(
          failedOrder.id,
          "payment-failed",
          failedOrder.totalCents,
          "capture_failed",
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          settledOrder.id,
          envelopeJson("PaymentCaptured", settledOrder.id, settledEvent.asJson)
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          failedOrder.id,
          envelopeJson("PaymentFailed", failedOrder.id, failedEvent.asJson)
        )
        result <- IO.race(
          PaymentEventConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
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
    "a synthetic PaymentCaptured envelope also writes an OrderStatusChanged outbox row with status=confirmed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        storeAndOutbox <- OrderStore.inMemoryWithOutbox[IO]
        (store, outboxRef) = storeAndOutbox
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Reserved)
        event = PaymentCapturedEvent(
          order.id,
          order.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          order.id,
          envelopeJson("PaymentCaptured", order.id, event.asJson)
        )
        outboxed <- IO.race(
          PaymentEventConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
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
              assertEquals(event.status, "confirmed")
          }
      }
    }
  }

  test(
    "a synthetic PaymentAuthorizationExpired envelope also writes an OrderStatusChanged outbox row with status=cancelled"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        storeAndOutbox <- OrderStore.inMemoryWithOutbox[IO]
        (store, outboxRef) = storeAndOutbox
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Reserved)
        event = PaymentAuthorizationExpiredEvent(
          order.id,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          order.id,
          envelopeJson(
            "PaymentAuthorizationExpired",
            order.id,
            event.asJson
          )
        )
        outboxed <- IO.race(
          PaymentEventConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
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
              assertEquals(event.status, "cancelled")
          }
      }
    }
  }

  test(
    "a synthetic PaymentFailed envelope also writes an OrderStatusChanged outbox row with status=payment_failed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        storeAndOutbox <- OrderStore.inMemoryWithOutbox[IO]
        (store, outboxRef) = storeAndOutbox
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Reserved)
        event = PaymentFailedEvent(
          order.id,
          "payment-1",
          order.totalCents,
          "capture_failed",
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          order.id,
          envelopeJson("PaymentFailed", order.id, event.asJson)
        )
        outboxed <- IO.race(
          PaymentEventConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
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
              assertEquals(event.status, "payment_failed")
          }
      }
    }
  }

  test(
    "a record with a null value doesn't crash the consumer, and a later valid event still updates order status (TD-2.2)"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- OrderStore.inMemory[IO]
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Reserved)
        event = PaymentCapturedEvent(
          order.id,
          order.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produceNullValue(config, PaymentEventConsumer.topic, order.id)
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          order.id,
          envelopeJson("PaymentCaptured", order.id, event.asJson)
        )
        finalStatus <- IO.race(
          PaymentEventConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.Confirmed)
        )
      } yield assertEquals(finalStatus, Right(OrderStatus.Confirmed))
    }
  }
}
