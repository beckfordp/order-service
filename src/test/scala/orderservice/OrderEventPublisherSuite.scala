package orderservice

import cats.effect.IO
import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import fs2.kafka._
import io.circe.parser.decode
import munit.CatsEffectSuite
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger.WARN

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration._

class OrderEventPublisherSuite
    extends CatsEffectSuite
    with TestContainerForAll {

  override val containerDef: KafkaContainer.Def = KafkaContainer.Def()

  private def configFor(kafka: KafkaContainer): KafkaConfig =
    KafkaConfig(bootstrapServers = kafka.bootstrapServers)

  /** Both event types now share one `order-events` topic (ADR 0009), so a
    * fresh "earliest" consumer can see another test's leftover message
    * first — filter for the envelope matching this test's own
    * `correlationId` rather than just taking the first record.
    */
  private def consumeEnvelope(
      config: KafkaConfig,
      topic: String,
      correlationId: String
  ): IO[String] = {
    val consumerSettings =
      ConsumerSettings[IO, String, String]
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(s"test-${UUID.randomUUID()}")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)

    KafkaConsumer.resource(consumerSettings).use { consumer =>
      for {
        _ <- consumer.subscribeTo(topic)
        record <- consumer.stream
          .map(_.record.value)
          .filter(raw =>
            decode[EventEnvelope](raw).exists(
              _.correlationId == correlationId
            )
          )
          .take(1)
          .compile
          .lastOrError
          .timeout(15.seconds)
      } yield record
    }
  }

  test(
    "publishReserved produces one message on order-events, wrapped in an envelope"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      val event = OrderReservedEvent(
        orderId = "order-reserved-1",
        customerId = "cust-123",
        totalCents = 4999,
        timestamp = Instant.parse("2026-01-01T00:00:00Z")
      )
      for {
        consumed <- OrderEventPublisher
          .resource[IO](config, NoOpLogger[IO])
          .use(_.publishReserved(event)) *> consumeEnvelope(
          config,
          OrderEventPublisher.topic,
          event.orderId
        )
        envelope = decode[EventEnvelope](consumed).toOption.get
      } yield {
        assertEquals(envelope.eventType, "OrderReserved")
        assertEquals(envelope.source, "order-service")
        assertEquals(envelope.correlationId, "order-reserved-1")
        assert(envelope.eventId.nonEmpty)
        assertEquals(envelope.payload.as[OrderReservedEvent], Right(event))
      }
    }
  }

  test(
    "publishStatusChanged produces one message on order-events, wrapped in an envelope"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      val event = OrderStatusChangedEvent(
        orderId = "order-status-1",
        customerId = "cust-123",
        status = "reservation_failed",
        timestamp = Instant.parse("2026-01-01T00:00:00Z")
      )
      for {
        consumed <- OrderEventPublisher
          .resource[IO](config, NoOpLogger[IO])
          .use(_.publishStatusChanged(event)) *> consumeEnvelope(
          config,
          OrderEventPublisher.topic,
          event.orderId
        )
        envelope = decode[EventEnvelope](consumed).toOption.get
      } yield {
        assertEquals(envelope.eventType, "OrderStatusChanged")
        assertEquals(envelope.source, "order-service")
        assertEquals(envelope.correlationId, "order-status-1")
        assert(envelope.eventId.nonEmpty)
        assertEquals(
          envelope.payload.as[OrderStatusChangedEvent],
          Right(event)
        )
      }
    }
  }

  test(
    "publishReserved logs a WARN and does not raise once the bounded retry is exhausted against an unreachable broker"
  ) {
    val unreachableConfig = KafkaConfig(bootstrapServers = "localhost:1")
    val event = OrderReservedEvent(
      orderId = "order-1",
      customerId = "cust-123",
      totalCents = 4999,
      timestamp = Instant.parse("2026-01-01T00:00:00Z")
    )
    val testLogger = StructuredTestingLogger.impl[IO]()
    for {
      result <- OrderEventPublisher
        .resource[IO](unreachableConfig, testLogger)
        .use(_.publishReserved(event))
        .attempt
      logged <- testLogger.logged
    } yield {
      assert(result.isRight, s"expected publish to not raise, got: $result")
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.nonEmpty,
        s"expected at least one WARN line, got: $logged"
      )
    }
  }
}
