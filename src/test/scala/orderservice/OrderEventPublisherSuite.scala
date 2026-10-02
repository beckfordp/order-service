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

class OrderEventPublisherSuite extends CatsEffectSuite with TestContainerForAll {

  override val containerDef: KafkaContainer.Def = KafkaContainer.Def()

  private def configFor(kafka: KafkaContainer): KafkaConfig =
    KafkaConfig(bootstrapServers = kafka.bootstrapServers)

  private def consumeOne(config: KafkaConfig, topic: String): IO[String] = {
    val consumerSettings =
      ConsumerSettings[IO, String, String]
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(s"test-${UUID.randomUUID()}")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)

    KafkaConsumer.resource(consumerSettings).use { consumer =>
      for {
        _ <- consumer.subscribeTo(topic)
        record <- consumer.stream
          .take(1)
          .compile
          .lastOrError
          .timeout(15.seconds)
      } yield record.record.value
    }
  }

  test(
    "publishReserved produces exactly one message on order.reserved"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      val event = OrderReservedEvent(
        orderId = "order-1",
        customerId = "cust-123",
        totalCents = 4999,
        timestamp = Instant.parse("2026-01-01T00:00:00Z")
      )
      for {
        consumed <- OrderEventPublisher
          .resource[IO](config, NoOpLogger[IO])
          .use(_.publishReserved(event)) *> consumeOne(
          config,
          OrderEventPublisher.reservedTopic
        )
      } yield assertEquals(decode[OrderReservedEvent](consumed), Right(event))
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
