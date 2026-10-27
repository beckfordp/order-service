package orderservice

import cats.effect.{Async, Resource}
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.Stream
import fs2.kafka.ProducerSettings
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.metrics.Meter
import purekafka.consumer.{DeadLetter, StreamSupervisor}

/** Wires `PaymentEventConsumer` (order-service's only consumer since
  * `StockEventConsumer` was retired, ADR 0018) in `purekafka`'s
  * `StreamSupervisor` with its own `DeadLetter` — TD-2.2/TD-2.4, ADR 0010. Any
  * future consumer gets its own supervisor and is merged in, so a crash in one
  * never restarts another. Extracted out of `Main.scala`'s own `val run` so
  * this wiring is directly testable, not just exercised incidentally by running
  * the whole service. `config.streamSupervisor`/`config.dlqRetry`/
  * `config.dlqPublishResilience` are all `application.conf`-driven (see
  * `OrderServiceConfig.KafkaConfig`) rather than hardcoded here.
  */
object KafkaConsumers {

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
      paymentDeadLetter <- Stream.resource(
        DeadLetter.resource[F, String, String](
          config.dlqRetry,
          config.dlqPublishResilience,
          dlqProducerSettings
        )(logger)(meter)
      )
      result <- StreamSupervisor.supervise[F, Unit](config.streamSupervisor)(
        logger
      )(meter)(
        Async[F].pure(
          PaymentEventConsumer.run[F](config, store, logger, paymentDeadLetter)
        )
      )
    } yield result
  }

  /** `supervised`, backgrounded as a fire-and-forget fiber - so `Main` wires
    * this in as one `Resource[F, Unit]` line, the same shape as every other
    * dependency, with no fs2 streaming mechanics visible there.
    */
  def resource[F[_]: Async](
      config: KafkaConfig,
      store: OrderStore[F],
      logger: StructuredLogger[F],
      meter: Meter[F]
  ): Resource[F, Unit] =
    supervised[F](config, store, logger, meter).compile.drain.background.void
}
