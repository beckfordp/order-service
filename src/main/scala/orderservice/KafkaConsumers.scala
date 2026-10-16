package orderservice

import cats.effect.Async
import fs2.Stream
import fs2.kafka.ProducerSettings
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.metrics.Meter
import purekafka.consumer.{DeadLetter, StreamSupervisor, StreamSupervisorConfig}
import purekafka.resilience.RetryConfig

import scala.concurrent.duration._

/** Wires `StockEventConsumer` and `PaymentEventConsumer` together, each
  * individually wrapped in `purekafka`'s `StreamSupervisor` *before* merging —
  * TD-2.2/TD-2.4, ADR 0010. Supervising the already-merged stream as one unit
  * would mean a crash in either consumer force-restarts both; wrapping each
  * separately means a crash in one doesn't touch the other's in-flight
  * processing. Extracted out of `Main.scala`'s own `val run` so this wiring is
  * directly testable, not just exercised incidentally by running the whole
  * service.
  */
object KafkaConsumers {

  // baseDelay/maxDelay match the cap purekafka's own StreamSupervisor track
  // decided (ADR 0010, Phase 2) — 30s is the ceiling exponential backoff
  // grows to, not something this service needed to re-decide.
  private val supervisorConfig = StreamSupervisorConfig(
    baseDelay = 1.second,
    maxDelay = 30.seconds
  )

  // Per-message retry before a message is dead-lettered — deliberately much
  // shorter/bounded than supervisorConfig above: this is "retry processing
  // this one message a few times," not "back off a whole crashed stream."
  private val dlqRetryConfig =
    RetryConfig(maxRetries = 3, baseDelay = 200.millis)

  def supervised[F[_]: Async](
      config: KafkaConfig,
      store: OrderStore[F],
      logger: StructuredLogger[F],
      meter: Meter[F]
  ): Stream[F, Unit] = {
    def dlqProducerSettings: ProducerSettings[F, String, String] =
      ProducerSettings[F, String, String]
        .withBootstrapServers(config.bootstrapServers)

    for {
      stockDeadLetter <- Stream.resource(
        DeadLetter.resource[F, String, String](
          dlqRetryConfig,
          dlqProducerSettings
        )(logger)(meter)
      )
      paymentDeadLetter <- Stream.resource(
        DeadLetter.resource[F, String, String](
          dlqRetryConfig,
          dlqProducerSettings
        )(logger)(meter)
      )
      result <- {
        val stock = StreamSupervisor.supervise[F, Unit](supervisorConfig)(
          logger
        )(meter)(
          Async[F].pure(
            StockEventConsumer.run[F](config, store, logger, stockDeadLetter)
          )
        )
        val payment = StreamSupervisor.supervise[F, Unit](supervisorConfig)(
          logger
        )(meter)(
          Async[F].pure(
            PaymentEventConsumer
              .run[F](config, store, logger, paymentDeadLetter)
          )
        )
        stock.merge(payment)
      }
    } yield result
  }
}
