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

  /** Polls an `inMemoryWithOutbox` ref for an outbox row matching `orderId`
    * and `eventType` - the outbox write (ADR 0008, TD-3.4) replaces the old
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
    "a synthetic PaymentSettled envelope flips a Reserved order's status to Confirmed"
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
          PaymentEventConsumer.topic,
          order.id,
          envelopeJson("PaymentSettled", order.id, event.asJson)
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
    "a synthetic PaymentFailed envelope flips a Reserved order's status to PaymentFailed"
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
          PaymentEventConsumer.topic,
          order.id,
          envelopeJson("PaymentFailed", order.id, event.asJson)
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
    "an envelope with a different eventType on payment-events is ignored, not treated as malformed"
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
          envelopeJson("PaymentSettled", order.id, realEvent.asJson)
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
          PaymentEventConsumer.topic,
          stalePendingOrder.id,
          envelopeJson(
            "PaymentSettled",
            stalePendingOrder.id,
            staleEvent.asJson
          )
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          liveOrder.id,
          envelopeJson("PaymentSettled", liveOrder.id, liveEvent.asJson)
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
          PaymentEventConsumer.topic,
          "unknown-order-id",
          envelopeJson(
            "PaymentSettled",
            "unknown-order-id",
            unknownEvent.asJson
          )
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          order.id,
          envelopeJson("PaymentSettled", order.id, realEvent.asJson)
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
    "a malformed envelope is a no-op and doesn't block later events"
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
          PaymentEventConsumer.topic,
          "bad-key",
          "{not valid json"
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          order.id,
          envelopeJson("PaymentSettled", order.id, realEvent.asJson)
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
    "PaymentSettled and PaymentFailed route to their own distinct target statuses, not the other's"
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
          PaymentEventConsumer.topic,
          settledOrder.id,
          envelopeJson("PaymentSettled", settledOrder.id, settledEvent.asJson)
        )
        _ <- produce(
          config,
          PaymentEventConsumer.topic,
          failedOrder.id,
          envelopeJson("PaymentFailed", failedOrder.id, failedEvent.asJson)
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
    "a synthetic PaymentSettled envelope also writes an OrderStatusChanged outbox row with status=confirmed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        storeAndOutbox <- OrderStore.inMemoryWithOutbox[IO]
        (store, outboxRef) = storeAndOutbox
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
          PaymentEventConsumer.topic,
          order.id,
          envelopeJson("PaymentSettled", order.id, event.asJson)
        )
        outboxed <- IO.race(
          PaymentEventConsumer
            .run[IO](
              config,
              store,
              NoOpLogger[IO],
              OrderEventPublisher.noOp[IO]
            )
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
    "a synthetic PaymentFailed envelope also writes an OrderStatusChanged outbox row with status=payment_failed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        storeAndOutbox <- OrderStore.inMemoryWithOutbox[IO]
        (store, outboxRef) = storeAndOutbox
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
          PaymentEventConsumer.topic,
          order.id,
          envelopeJson("PaymentFailed", order.id, event.asJson)
        )
        outboxed <- IO.race(
          PaymentEventConsumer
            .run[IO](
              config,
              store,
              NoOpLogger[IO],
              OrderEventPublisher.noOp[IO]
            )
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
}
