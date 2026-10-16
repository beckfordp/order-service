package orderservice

import cats.effect.{IO, Ref}
import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import fs2.kafka._
import io.circe.Json
import io.circe.syntax._
import munit.CatsEffectSuite
import org.typelevel.log4cats.noop.NoOpLogger
import purerest.metrics.Metrics

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Proves the per-consumer supervision property `KafkaConsumers.supervised`
  * exists for (TD-2.2/TD-2.4, ADR 0010): wrapping each of
  * `StockEventConsumer`/`PaymentEventConsumer` in `StreamSupervisor`
  * *individually*, then merging, means a crash in one doesn't restart or
  * otherwise affect the other's in-flight processing — unlike supervising
  * the already-merged stream as one unit.
  */
class KafkaConsumersSuite extends CatsEffectSuite with TestContainerForAll {

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
      source: String,
      eventType: String,
      correlationId: String,
      payload: Json
  ): String =
    EventEnvelope(
      eventId = UUID.randomUUID().toString,
      eventType = eventType,
      timestamp = Instant.parse("2026-01-01T00:00:00Z"),
      source = source,
      correlationId = correlationId,
      payload = payload
    ).asJson.noSpaces

  private def pollUntil[A](check: IO[Option[A]]): IO[A] =
    check.flatMap {
      case Some(a) => IO.pure(a)
      case None    => IO.sleep(200.millis) *> pollUntil(check)
    }

  private def statusOf(store: OrderStore[IO], orderId: String): IO[OrderStatus] =
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
    ).timeout(20.seconds)

  private val oneItem = List(NewOrderItem("sku-1", "Widget", 999, 2))

  /** Delegates everything to `base`, except `updateStatusByItemId` for
    * `failOnItemId` - that call raises exactly once (simulating a transient
    * failure, e.g. a DB blip), then succeeds on retry. Crashes
    * `StockEventConsumer`'s own `evalMap` for that one event, which is what
    * should trigger `StreamSupervisor`'s restart.
    */
  private def failOnceOn(
      base: OrderStore[IO],
      failOnItemId: String,
      alreadyFailed: Ref[IO, Boolean]
  ): OrderStore[IO] = new OrderStore[IO] {
    def create(customerId: String, items: List[NewOrderItem]) =
      base.create(customerId, items)
    def get(id: String) = base.get(id)
    def listByCustomer(customerId: String) = base.listByCustomer(customerId)
    def update(
        id: String,
        status: OrderStatus,
        outboxEvent: Option[PendingOutboxEvent] = None
    ) = base.update(id, status, outboxEvent)
    def updateStatusByItemId(
        orderItemId: String,
        newStatus: OrderStatus,
        outboxEvent: Option[PendingOutboxEvent] = None
    ) =
      if (orderItemId == failOnItemId)
        alreadyFailed.get.flatMap {
          case true  => base.updateStatusByItemId(orderItemId, newStatus, outboxEvent)
          case false =>
            alreadyFailed.set(true) *> IO.raiseError(
              new RuntimeException("synthetic transient store failure")
            )
        }
      else base.updateStatusByItemId(orderItemId, newStatus, outboxEvent)
    def updateStatusIfCurrent(
        id: String,
        expected: OrderStatus,
        newStatus: OrderStatus,
        outboxEvent: Option[PendingOutboxEvent] = None
    ) = base.updateStatusIfCurrent(id, expected, newStatus, outboxEvent)
    def delete(id: String) = base.delete(id)
    def ping = base.ping
  }

  test(
    "a crash in StockEventConsumer's stream restarts it (via purekafka's StreamSupervisor) without affecting PaymentEventConsumer's own, concurrently-processing stream (TD-2.2/TD-2.4)"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      Metrics.test[IO]("kafka-consumers-suite").use { testMeter =>
        for {
          baseStore <- OrderStore.inMemory[IO]
          alreadyFailed <- Ref.of[IO, Boolean](false)
          (stockOrder, stockItems) <- baseStore.create("cust-stock", oneItem)
          failingItemId = stockItems.head.id
          store = failOnceOn(baseStore, failingItemId, alreadyFailed)
          (paymentOrder, _) <- baseStore.create("cust-payment", oneItem)
          _ <- baseStore.update(paymentOrder.id, OrderStatus.Reserved)
          stockEvent = StockReservedEvent(
            failingItemId,
            "sku-1",
            2,
            Instant.parse("2026-01-01T00:00:00Z")
          )
          paymentEvent = PaymentSettledEvent(
            paymentOrder.id,
            "payment-1",
            paymentOrder.totalCents,
            Instant.parse("2026-01-01T00:00:00Z")
          )
          _ <- produce(
            config,
            StockEventConsumer.topic,
            "sku-1",
            envelopeJson(
              "inventory-service",
              "StockReserved",
              failingItemId,
              stockEvent.asJson
            )
          )
          _ <- produce(
            config,
            PaymentEventConsumer.topic,
            paymentOrder.id,
            envelopeJson(
              "payment-service",
              "PaymentSettled",
              paymentOrder.id,
              paymentEvent.asJson
            )
          )
          // Run the merged, supervised stream in the background rather than
          // racing it: a race would cancel the whole stream (including
          // StockEventConsumer's not-yet-finished restart-then-retry
          // sequence) the instant the *first* of two awaited conditions
          // resolves, which is exactly what happened on the first attempt
          // at this test (PaymentEventConsumer's immediate success raced
          // out StockEventConsumer's still-in-flight retry).
          (paymentResult, stockFinal) <-
            KafkaConsumers
              .supervised[IO](config, store, NoOpLogger[IO], testMeter.meter)
              .compile
              .drain
              .background
              .use { _ =>
                // PaymentEventConsumer's own stream is never touched by the
                // StockEventConsumer-side crash+restart above - if it were,
                // this poll would stall behind Stock's ~1s restart delay
                // instead of completing immediately. IO.both runs both
                // polls concurrently rather than one after the other, so
                // Payment's quick success is actually observed while Stock
                // is still mid-retry, not just after it too has finished.
                IO.both(
                  pollForStatus(store, paymentOrder.id, OrderStatus.Confirmed)
                    .timeout(20.seconds),
                  pollForStatus(store, stockOrder.id, OrderStatus.Reserved)
                    .timeout(20.seconds)
                )
              }
          metrics <- testMeter.collectMetrics
          restarts = metrics
            .filter(_.getName == "purekafka.stream.restarts")
            .flatMap(_.getLongSumData.getPoints.asScala)
            .map(_.getValue)
            .sum
        } yield {
          assertEquals(paymentResult, OrderStatus.Confirmed)
          assertEquals(stockFinal, OrderStatus.Reserved)
          assert(
            restarts >= 1L,
            s"expected at least 1 purekafka.stream.restarts, got $restarts"
          )
        }
      }
    }
  }
}
