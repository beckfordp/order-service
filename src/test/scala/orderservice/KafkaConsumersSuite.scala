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
  * `PaymentEventConsumer` in `StreamSupervisor` (its only consumer since
  * `StockEventConsumer` was retired, ADR 0018) keeps a crash or poison message
  * from stopping later processing.
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
    ).timeout(20.seconds)

  private val oneItem = List(NewOrderItem("sku-1", "Widget", 999, 2))

  test(
    "wrapping two streams individually in StreamSupervisor, then merging, isolates a crash in one from the other (TD-2.2/TD-2.4) - the exact wiring pattern KafkaConsumers.supervised uses"
  ) {
    // A *synthetic*-stream test, not a real-consumer one, by necessity: once
    // DeadLetter wraps every processing branch (below, this same phase), it
    // absorbs essentially all application-level failures internally (retry,
    // then DLQ-and-swallow on exhaustion) before they ever reach
    // StreamSupervisor - confirmed empirically: the original version of this
    // test, built on a failing-once OrderStore decorator exactly like Phase
    // 1/2's own tests use, now observes zero restarts, because DeadLetter's
    // own bounded retry catches the one-time failure first. That's a strictly
    // better real-world outcome (fewer unnecessary whole-stream restarts),
    // but it means a real consumer's business logic is no longer a
    // reliable way to *trigger* a StreamSupervisor restart in a fast,
    // deterministic unit test. purekafka's own StreamSupervisorSuite already
    // proves restart-with-backoff against a synthetic Stream[F, O] for
    // exactly this reason - this test proves the *merge-isolation* property
    // the same way, directly, rather than depending on real Kafka I/O or
    // real business logic having an exploitable crash path.
    Metrics.test[IO]("kafka-consumers-suite").use { testMeter =>
      for {
        attempts <- Ref.of[IO, Int](0)
        healthyRuns <- Ref.of[IO, Int](0)
        failingBuild: IO[fs2.Stream[IO, Unit]] = attempts
          .updateAndGet(_ + 1)
          .map { attempt =>
            if (attempt < 3)
              fs2.Stream.raiseError[IO](
                new RuntimeException(s"synthetic crash, attempt $attempt")
              )
            else fs2.Stream.sleep[IO](10.seconds) // never completes on its own
          }
        healthyBuild: IO[fs2.Stream[IO, Unit]] = IO.pure(
          fs2.Stream.eval(healthyRuns.update(_ + 1)) >> fs2.Stream.sleep[IO](
            10.seconds
          )
        )
        supervisorConfig = purekafka.consumer.StreamSupervisorConfig(
          baseDelay = 50.millis,
          maxDelay = 200.millis
        )
        failing = purekafka.consumer.StreamSupervisor.supervise[IO, Unit](
          supervisorConfig
        )(NoOpLogger[IO])(testMeter.meter)(failingBuild)
        healthy = purekafka.consumer.StreamSupervisor.supervise[IO, Unit](
          supervisorConfig
        )(NoOpLogger[IO])(testMeter.meter)(healthyBuild)
        merged = failing.merge(healthy)
        _ <- merged.compile.drain.background.use { _ =>
          // The healthy stream should start running almost immediately,
          // regardless of the failing one's restart loop - if merging
          // supervised the *combined* stream as one unit instead of each
          // individually, the failing stream's repeated crashes would keep
          // tearing down and restarting the healthy one too, and
          // healthyRuns would climb in lockstep with the failing stream's
          // own restart count instead of settling at 1 immediately.
          IO.sleep(500.millis)
        }
        finalAttempts <- attempts.get
        finalHealthyRuns <- healthyRuns.get
        metrics <- testMeter.collectMetrics
        restarts = metrics
          .filter(_.getName == "purekafka.stream.restarts")
          .flatMap(_.getLongSumData.getPoints.asScala)
          .map(_.getValue)
          .sum
      } yield {
        assert(
          finalAttempts >= 3,
          s"expected the failing stream to have been (re)built at least 3 times, got $finalAttempts"
        )
        assertEquals(
          finalHealthyRuns,
          1,
          "the healthy stream should only have started once - a restart in the failing stream must not have torn it down"
        )
        assert(
          restarts >= 1L,
          s"expected at least 1 purekafka.stream.restarts, got $restarts"
        )
      }
    }
  }

  test(
    "a message that fails processing every time is routed to <topic>-dlq after exhausting retries, and the stream keeps consuming later messages (TD-2.4)"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      Metrics.test[IO]("kafka-consumers-dlq-suite").use { testMeter =>
        for {
          baseStore <- OrderStore.inMemory[IO]
          (poisonOrder, _) <- baseStore.create("cust-poison", oneItem)
          (healthyOrder, _) <- baseStore.create("cust-healthy", oneItem)
          _ <- baseStore.update(poisonOrder.id, OrderStatus.Authorized)
          _ <- baseStore.update(healthyOrder.id, OrderStatus.Authorized)
          store = alwaysFailOn(baseStore, poisonOrder.id)
          captured = (orderId: String) =>
            envelopeJson(
              "payment-service",
              "PaymentCaptured",
              orderId,
              PaymentCapturedEvent(
                orderId,
                1998,
                Instant.parse("2026-01-01T00:00:00Z")
              ).asJson
            )
          _ <- produce(
            config,
            PaymentEventConsumer.topic,
            poisonOrder.id,
            captured(poisonOrder.id)
          )
          _ <- produce(
            config,
            PaymentEventConsumer.topic,
            healthyOrder.id,
            captured(healthyOrder.id)
          )
          healthyFinal <-
            KafkaConsumers
              .supervised[IO](
                config,
                store,
                new InventoryClient[IO] {
                  def reserveOrder(
                      orderId: String,
                      items: List[OrderReservationLine]
                  ): IO[OrderReservationResult] =
                    IO.raiseError(new NotImplementedError())
                  def release(orderId: String): IO[Unit] = IO.unit
                },
                NoOpLogger[IO],
                testMeter.meter
              )
              .compile
              .drain
              .background
              .use { _ =>
                // Proves the stream didn't get stuck/killed on the poison
                // message - a later, healthy message on the same topic
                // still gets processed and updates order status normally.
                pollForStatus(store, healthyOrder.id, OrderStatus.Confirmed)
                  .timeout(20.seconds)
              }
          dlqMessages <- consumeOneFrom(
            config,
            s"${PaymentEventConsumer.topic}-dlq"
          )
          metrics <- testMeter.collectMetrics
          dlqPublished = metrics
            .filter(_.getName == "purekafka.dlq.published")
            .flatMap(_.getLongSumData.getPoints.asScala)
            .map(_.getValue)
            .sum
        } yield {
          assertEquals(healthyFinal, OrderStatus.Confirmed)
          assert(
            dlqMessages.exists(_.contains(poisonOrder.id)),
            s"expected a DLQ message mentioning ${poisonOrder.id}, got $dlqMessages"
          )
          assert(
            dlqPublished >= 1L,
            s"expected at least 1 purekafka.dlq.published, got $dlqPublished"
          )
        }
      }
    }
  }

  /** Like `failOnceOn`, but fails every time for `failOnOrderId` - never
    * recovers. Used to prove `DeadLetter` actually exhausts its retry budget
    * and routes to the DLQ, rather than eventually succeeding.
    */
  private def alwaysFailOn(
      base: OrderStore[IO],
      failOnOrderId: String
  ): OrderStore[IO] = new OrderStore[IO] {
    def create(customerId: String, items: List[NewOrderItem]) =
      base.create(customerId, items)
    def createAuthorized(order: Order, items: List[OrderItem]) =
      base.createAuthorized(order, items)
    def get(id: String) = base.get(id)
    def listByCustomer(customerId: String) = base.listByCustomer(customerId)
    def update(
        id: String,
        status: OrderStatus,
        outboxEvent: Option[PendingOutboxEvent] = None
    ) = base.update(id, status, outboxEvent)
    def updateStatusIfCurrent(
        id: String,
        expected: OrderStatus,
        newStatus: OrderStatus,
        outboxEvent: Option[PendingOutboxEvent] = None
    ) =
      if (id == failOnOrderId)
        IO.raiseError(
          new RuntimeException("synthetic permanent store failure")
        )
      else base.updateStatusIfCurrent(id, expected, newStatus, outboxEvent)
    def recordCaptureOutcome(
        id: String,
        newStatus: OrderStatus,
        outboxEvent: PendingOutboxEvent,
        reconciliationReason: Option[String]
    ) =
      if (id == failOnOrderId)
        IO.raiseError(
          new RuntimeException("synthetic permanent store failure")
        )
      else
        base.recordCaptureOutcome(
          id,
          newStatus,
          outboxEvent,
          reconciliationReason
        )
    def reconciliationReason(id: String) = base.reconciliationReason(id)
    def delete(id: String) = base.delete(id)
    def ping = base.ping
  }

  /** Consumes whatever's on `topic` right now (from the beginning) within a
    * short window, for asserting on DLQ contents directly - a plain, one-shot
    * read, not a long-running consumer group.
    */
  private def consumeOneFrom(
      config: KafkaConfig,
      topic: String
  ): IO[List[String]] = {
    val consumerSettings =
      ConsumerSettings[IO, String, String]
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(s"dlq-check-${UUID.randomUUID()}")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
    KafkaConsumer
      .stream(consumerSettings)
      .subscribeTo(topic)
      .records
      .map(_.record.value)
      .interruptAfter(5.seconds)
      .compile
      .toList
  }
}
