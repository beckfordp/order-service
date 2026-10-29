package orderservice

import cats.effect.IO
import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import fs2.kafka._
import io.circe.Json
import io.circe.parser.decode
import io.circe.syntax._
import munit.CatsEffectSuite
import orderservice.domain.OrderId
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger.{ERROR, INFO}
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
      orderId: OrderId
  ): IO[OrderStatus] =
    store.get(orderId).map(_.map(_._1.status).get)

  private def pollForStatus(
      store: OrderStore[IO],
      orderId: OrderId,
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
      orderId: OrderId,
      eventType: String
  ): IO[EventEnvelope] =
    pollUntil(
      outboxRef.get.map(
        _.collectFirst {
          case (id, envelope)
              if id == OrderId.render(orderId) &&
                envelope.eventType == eventType =>
            envelope
        }
      )
    ).timeout(15.seconds)

  private val oneItem =
    List(NewOrderItem("sku-1", "Widget", 999, 2))

  test(
    "a synthetic PaymentCaptured envelope flips a Dispatched order's status to Confirmed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- OrderStore.inMemory[IO]
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Dispatched)
        event = PaymentCapturedEvent(
          OrderId.render(order.id),
          order.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          OrderId.render(order.id),
          envelopeJson(
            "PaymentCaptured",
            OrderId.render(order.id),
            event.asJson
          )
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
    "a synthetic PaymentAuthorizationExpired envelope flips an Authorized order's status to Cancelled"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- OrderStore.inMemory[IO]
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Authorized)
        event = PaymentAuthorizationExpiredEvent(
          OrderId.render(order.id),
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          OrderId.render(order.id),
          envelopeJson(
            "PaymentAuthorizationExpired",
            OrderId.render(order.id),
            event.asJson
          )
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
    "a synthetic PaymentFailed envelope flips a Dispatched order's status to PaymentFailed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- OrderStore.inMemory[IO]
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Dispatched)
        event = PaymentFailedEvent(
          OrderId.render(order.id),
          "payment-1",
          order.totalCents,
          "capture_failed",
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          OrderId.render(order.id),
          envelopeJson("PaymentFailed", OrderId.render(order.id), event.asJson)
        )
        finalStatus <- IO.race(
          PaymentEventConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.PaymentFailed)
        )
        reason <- store.reconciliationReason(order.id)
      } yield {
        assertEquals(finalStatus, Right(OrderStatus.PaymentFailed))
        assertEquals(reason, Some("capture_failed"))
      }
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
        testLogger = StructuredTestingLogger.impl[IO]()
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Authorized)
        realEvent = PaymentCapturedEvent(
          OrderId.render(order.id),
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
          OrderId.render(order.id),
          envelopeJson(
            "PaymentCaptured",
            OrderId.render(order.id),
            realEvent.asJson
          )
        )
        finalStatus <- IO.race(
          PaymentEventConsumer
            .run[IO](config, store, testLogger, deadLetter)
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.Confirmed)
        )
        logged <- testLogger.logged
      } yield {
        assertEquals(finalStatus, Right(OrderStatus.Confirmed))
        val infos = logged.collect { case m: INFO => m }
        assert(
          infos.exists(_.ctx.get("event_type").contains("PaymentRefunded")),
          s"expected an info line naming the ignored event type, got: $infos"
        )
        assert(
          logged.forall(m => !m.ctx.contains("raw")),
          s"raw payloads must never be logged, got: $logged"
        )
      }
    }
  }

  test(
    "an event for an order that's not awaiting capture is a no-op and doesn't block later events"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- OrderStore.inMemory[IO]
        (stalePendingOrder, _) <- store.create("cust-123", oneItem)
        (liveOrder, _) <- store.create("cust-456", oneItem)
        _ <- store.update(liveOrder.id, OrderStatus.Authorized)
        staleEvent = PaymentCapturedEvent(
          OrderId.render(stalePendingOrder.id),
          stalePendingOrder.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        liveEvent = PaymentCapturedEvent(
          OrderId.render(liveOrder.id),
          liveOrder.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          OrderId.render(stalePendingOrder.id),
          envelopeJson(
            "PaymentCaptured",
            OrderId.render(stalePendingOrder.id),
            staleEvent.asJson
          )
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          OrderId.render(liveOrder.id),
          envelopeJson(
            "PaymentCaptured",
            OrderId.render(liveOrder.id),
            liveEvent.asJson
          )
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
        _ <- store.update(order.id, OrderStatus.Authorized)
        unknownEvent = PaymentCapturedEvent(
          "unknown-order-id",
          999,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        realEvent = PaymentCapturedEvent(
          OrderId.render(order.id),
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
          OrderId.render(order.id),
          envelopeJson(
            "PaymentCaptured",
            OrderId.render(order.id),
            realEvent.asJson
          )
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
        testLogger = StructuredTestingLogger.impl[IO]()
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Authorized)
        realEvent = PaymentCapturedEvent(
          OrderId.render(order.id),
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
          OrderId.render(order.id),
          envelopeJson(
            "PaymentCaptured",
            OrderId.render(order.id),
            realEvent.asJson
          )
        )
        finalStatus <- IO.race(
          PaymentEventConsumer
            .run[IO](config, store, testLogger, deadLetter)
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.Confirmed)
        )
        logged <- testLogger.logged
      } yield {
        assertEquals(finalStatus, Right(OrderStatus.Confirmed))
        val errors = logged.collect { case m: ERROR => m }
        assert(
          errors.exists(m =>
            m.ctx.get("topic").contains(PaymentEventConsumer.topic) &&
              m.ctx.contains("bytes")
          ),
          s"expected a decode error with topic/bytes context, got: $errors"
        )
        assert(
          logged.forall(m => !m.ctx.contains("raw")),
          s"raw payloads must never be logged, got: $logged"
        )
      }
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
        _ <- store.update(settledOrder.id, OrderStatus.Dispatched)
        (failedOrder, _) <- store.create("cust-456", oneItem)
        _ <- store.update(failedOrder.id, OrderStatus.Dispatched)
        settledEvent = PaymentCapturedEvent(
          OrderId.render(settledOrder.id),
          settledOrder.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        failedEvent = PaymentFailedEvent(
          OrderId.render(failedOrder.id),
          "payment-failed",
          failedOrder.totalCents,
          "capture_failed",
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          OrderId.render(settledOrder.id),
          envelopeJson(
            "PaymentCaptured",
            OrderId.render(settledOrder.id),
            settledEvent.asJson
          )
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          OrderId.render(failedOrder.id),
          envelopeJson(
            "PaymentFailed",
            OrderId.render(failedOrder.id),
            failedEvent.asJson
          )
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
    "a synthetic PaymentCaptured envelope also writes an OrderConfirmed outbox row"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        storeAndOutbox <- OrderStore.inMemoryWithOutbox[IO]
        (store, outboxRef) = storeAndOutbox
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Dispatched)
        event = PaymentCapturedEvent(
          OrderId.render(order.id),
          order.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          OrderId.render(order.id),
          envelopeJson(
            "PaymentCaptured",
            OrderId.render(order.id),
            event.asJson
          )
        )
        outboxed <- IO.race(
          PaymentEventConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
            .compile
            .drain,
          pollForOutboxEvent(outboxRef, order.id, "OrderConfirmed")
        )
      } yield outboxed match {
        case Left(()) =>
          fail(
            "consumer finished before the OrderConfirmed outbox row appeared"
          )
        case Right(envelope) =>
          envelope.payload.as[OrderConfirmedEvent] match {
            case Left(error)  => fail(s"failed to decode: $error")
            case Right(event) =>
              assertEquals(event.orderId, OrderId.render(order.id))
              assertEquals(event.customerId, "cust-123")
          }
      }
    }
  }

  test(
    "a synthetic PaymentAuthorizationExpired envelope also writes an OrderCancelled outbox row"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        storeAndOutbox <- OrderStore.inMemoryWithOutbox[IO]
        (store, outboxRef) = storeAndOutbox
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Authorized)
        event = PaymentAuthorizationExpiredEvent(
          OrderId.render(order.id),
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          OrderId.render(order.id),
          envelopeJson(
            "PaymentAuthorizationExpired",
            OrderId.render(order.id),
            event.asJson
          )
        )
        outboxed <- IO.race(
          PaymentEventConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
            .compile
            .drain,
          pollForOutboxEvent(outboxRef, order.id, "OrderCancelled")
        )
      } yield outboxed match {
        case Left(()) =>
          fail(
            "consumer finished before the OrderCancelled outbox row appeared"
          )
        case Right(envelope) =>
          envelope.payload.as[OrderCancelledEvent] match {
            case Left(error)  => fail(s"failed to decode: $error")
            case Right(event) =>
              assertEquals(event.orderId, OrderId.render(order.id))
              assertEquals(event.customerId, "cust-123")
          }
      }
    }
  }

  test(
    "a synthetic PaymentFailed envelope also writes an OrderPaymentFailed outbox row with its reason"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        storeAndOutbox <- OrderStore.inMemoryWithOutbox[IO]
        (store, outboxRef) = storeAndOutbox
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Dispatched)
        event = PaymentFailedEvent(
          OrderId.render(order.id),
          "payment-1",
          order.totalCents,
          "capture_failed",
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          OrderId.render(order.id),
          envelopeJson("PaymentFailed", OrderId.render(order.id), event.asJson)
        )
        outboxed <- IO.race(
          PaymentEventConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
            .compile
            .drain,
          pollForOutboxEvent(outboxRef, order.id, "OrderPaymentFailed")
        )
      } yield outboxed match {
        case Left(()) =>
          fail(
            "consumer finished before the OrderPaymentFailed outbox row appeared"
          )
        case Right(envelope) =>
          envelope.payload.as[OrderPaymentFailedEvent] match {
            case Left(error)  => fail(s"failed to decode: $error")
            case Right(event) =>
              assertEquals(event.orderId, OrderId.render(order.id))
              assertEquals(event.customerId, "cust-123")
              assertEquals(event.reason, "capture_failed")
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
        _ <- store.update(order.id, OrderStatus.Authorized)
        event = PaymentCapturedEvent(
          OrderId.render(order.id),
          order.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produceNullValue(
          config,
          PaymentEventConsumer.topic,
          OrderId.render(order.id)
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          OrderId.render(order.id),
          envelopeJson(
            "PaymentCaptured",
            OrderId.render(order.id),
            event.asJson
          )
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
    "PaymentCaptured also flips an Authorized order when it overtakes DispatchConfirmed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- OrderStore.inMemory[IO]
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Authorized)
        event = PaymentCapturedEvent(
          OrderId.render(order.id),
          order.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          OrderId.render(order.id),
          envelopeJson(
            "PaymentCaptured",
            OrderId.render(order.id),
            event.asJson
          )
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
    "PaymentFailed also flips an Authorized order when it overtakes DispatchConfirmed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- OrderStore.inMemory[IO]
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Authorized)
        event = PaymentFailedEvent(
          OrderId.render(order.id),
          "payment-1",
          order.totalCents,
          "capture_failed",
          Instant.parse("2026-01-01T00:00:00Z")
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          OrderId.render(order.id),
          envelopeJson("PaymentFailed", OrderId.render(order.id), event.asJson)
        )
        finalStatus <- IO.race(
          PaymentEventConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.PaymentFailed)
        )
        reason <- store.reconciliationReason(order.id)
      } yield {
        assertEquals(finalStatus, Right(OrderStatus.PaymentFailed))
        assertEquals(reason, Some("capture_failed"))
      }
    }
  }

  test("a duplicate PaymentCaptured after confirmation is a no-op") {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        storeAndOutbox <- OrderStore.inMemoryWithOutbox[IO]
        (store, outboxRef) = storeAndOutbox
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Dispatched)
        event = PaymentCapturedEvent(
          OrderId.render(order.id),
          order.totalCents,
          Instant.parse("2026-01-01T00:00:00Z")
        )
        json = envelopeJson(
          "PaymentCaptured",
          OrderId.render(order.id),
          event.asJson
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          OrderId.render(order.id),
          json
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          OrderId.render(order.id),
          json
        )
        _ <- PaymentEventConsumer
          .run[IO](config, store, NoOpLogger[IO], deadLetter)
          .compile
          .drain
          .background
          .use(_ => IO.sleep(4.seconds))
        status <- statusOf(store, order.id)
        events <- outboxRef.get
      } yield {
        assertEquals(status, OrderStatus.Confirmed)
        assertEquals(
          events.count { case (id, envelope) =>
            id == OrderId.render(
              order.id
            ) && envelope.eventType == "OrderConfirmed"
          },
          1
        )
      }
    }
  }
}
