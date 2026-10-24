package orderservice

import cats.effect.{Async, Resource}
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.Stream
import fs2.kafka.ProducerSettings
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.metrics.Meter
import purekafka.consumer.{DeadLetter, StreamSupervisor}

/** Wires `StockEventConsumer` and `PaymentEventConsumer` together, each
  * individually wrapped in `purekafka`'s `StreamSupervisor` *before* merging —
  * TD-2.2/TD-2.4, ADR 0010. Supervising the already-merged stream as one unit
  * would mean a crash in either consumer force-restarts both; wrapping each
  * separately means a crash in one doesn't touch the other's in-flight
  * processing. Extracted out of `Main.scala`'s own `val run` so this wiring is
  * directly testable, not just exercised incidentally by running the whole
  * service. `config.streamSupervisor`/`config.dlqRetry`/
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
      stockDeadLetter <- Stream.resource(
        DeadLetter.resource[F, String, String](
          config.dlqRetry,
          config.dlqPublishResilience,
          dlqProducerSettings
        )(logger)(meter)
      )
      paymentDeadLetter <- Stream.resource(
        DeadLetter.resource[F, String, String](
          config.dlqRetry,
          config.dlqPublishResilience,
          dlqProducerSettings
        )(logger)(meter)
      )
      result <- {
        val stock =
          StreamSupervisor.supervise[F, Unit](config.streamSupervisor)(
            logger
          )(meter)(
            Async[F].pure(
              StockEventConsumer.run[F](config, store, logger, stockDeadLetter)
            )
          )
        val payment =
          StreamSupervisor.supervise[F, Unit](config.streamSupervisor)(
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
