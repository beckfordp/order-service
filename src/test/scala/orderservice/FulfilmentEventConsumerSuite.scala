package orderservice

import cats.effect.{IO, Ref}
import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import fs2.kafka._
import io.circe.Json
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

class FulfilmentEventConsumerSuite
    extends CatsEffectSuite
    with TestContainerForAll {

  override val containerDef: KafkaContainer.Def = KafkaContainer.Def()

  private def configFor(kafka: KafkaContainer): KafkaConfig =
    KafkaConfig(bootstrapServers = kafka.bootstrapServers)

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

  private def produce(
      config: KafkaConfig,
      key: String,
      json: String
  ): IO[Unit] = {
    val producerSettings =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(config.bootstrapServers)
    KafkaProducer
      .resource(producerSettings)
      .use(
        _.produceOne_(
          ProducerRecord(FulfilmentEventConsumer.topic, key, json)
        ).flatten.void
      )
  }

  private def envelopeJson(eventType: String, payload: Json): String =
    EventEnvelope(
      eventId = UUID.randomUUID().toString(),
      eventType = eventType,
      timestamp = Instant.parse("2026-01-01T00:00:00Z"),
      source = "fulfilment-service",
      correlationId = "order-1",
      payload = payload
    ).asJson.noSpaces

  private def inventory(
      released: Ref[IO, List[String]]
  ): InventoryClient[IO] = new InventoryClient[IO] {
    def reserveOrder(
        orderId: String,
        items: List[OrderReservationLine]
    ): IO[OrderReservationResult] =
      IO.raiseError(new NotImplementedError())
    def release(orderId: String): IO[Unit] =
      released.update(_ :+ orderId)
  }

  private val oneItem = List(NewOrderItem("sku-1", "Widget", 999, 2))

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
    statusOf(store, orderId)
      .flatMap { status =>
        if (status == expected) IO.pure(status)
        else IO.sleep(200.millis) *> pollForStatus(store, orderId, expected)
      }
      .timeout(15.seconds)

  test(
    "a synthetic DispatchConfirmed moves an Authorized order to Dispatched"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- OrderStore.inMemory[IO]
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Authorized)
        event = DispatchConfirmedEvent(
          order.id,
          "GLN-1",
          List(FulfilmentItem("sku-1", "Widget", 2, 999))
        )
        released <- Ref.of[IO, List[String]](Nil)
        _ <- produce(
          config,
          order.id,
          envelopeJson("DispatchConfirmed", event.asJson)
        )
        finalStatus <- IO.race(
          FulfilmentEventConsumer
            .run[IO](
              config,
              store,
              inventory(released),
              NoOpLogger[IO],
              deadLetter
            )
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.Dispatched)
        )
      } yield assertEquals(finalStatus, Right(OrderStatus.Dispatched))
    }
  }

  test("a DispatchConfirmed for a non-authorized order is skipped") {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- OrderStore.inMemory[IO]
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Cancelled)
        event = DispatchConfirmedEvent(
          order.id,
          "GLN-2",
          List(FulfilmentItem("sku-1", "Widget", 2, 999))
        )
        released <- Ref.of[IO, List[String]](Nil)
        _ <- produce(
          config,
          order.id,
          envelopeJson("DispatchConfirmed", event.asJson)
        )
        _ <- FulfilmentEventConsumer
          .run[IO](
            config,
            store,
            inventory(released),
            NoOpLogger[IO],
            deadLetter
          )
          .compile
          .drain
          .background
          .use(_ => IO.sleep(3.seconds))
        status <- statusOf(store, order.id)
      } yield assertEquals(status, OrderStatus.Cancelled)
    }
  }

  test(
    "a synthetic DispatchFailed moves an Authorized order and releases once"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- OrderStore.inMemory[IO]
        released <- Ref.of[IO, List[String]](Nil)
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Authorized)
        event = DispatchFailedEvent(order.id, "damaged")
        _ <- produce(
          config,
          order.id,
          envelopeJson("DispatchFailed", event.asJson)
        )
        finalStatus <- IO.race(
          FulfilmentEventConsumer
            .run[IO](
              config,
              store,
              inventory(released),
              NoOpLogger[IO],
              deadLetter
            )
            .compile
            .drain,
          pollForStatus(store, order.id, OrderStatus.DispatchFailed)
        )
        releases <- released.get
      } yield {
        assertEquals(finalStatus, Right(OrderStatus.DispatchFailed))
        assertEquals(releases, List(order.id))
      }
    }
  }

  test(
    "a DispatchFailed for a non-authorized order is skipped without releasing"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- OrderStore.inMemory[IO]
        released <- Ref.of[IO, List[String]](Nil)
        (order, _) <- store.create("cust-123", oneItem)
        _ <- store.update(order.id, OrderStatus.Dispatched)
        event = DispatchFailedEvent(order.id, "damaged")
        _ <- produce(
          config,
          order.id,
          envelopeJson("DispatchFailed", event.asJson)
        )
        _ <- FulfilmentEventConsumer
          .run[IO](
            config,
            store,
            inventory(released),
            NoOpLogger[IO],
            deadLetter
          )
          .compile
          .drain
          .background
          .use(_ => IO.sleep(3.seconds))
        status <- statusOf(store, order.id)
        releases <- released.get
      } yield {
        assertEquals(status, OrderStatus.Dispatched)
        assertEquals(releases, Nil)
      }
    }
  }
}
