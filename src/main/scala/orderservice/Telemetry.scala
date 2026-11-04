package orderservice

import cats.effect.{Async, Resource, Sync}
import cats.effect.unsafe.metrics.IORuntimeMetrics
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.metrics.Meter
import org.typelevel.otel4s.oteljava.context.LocalContextProvider
import org.typelevel.otel4s.trace.Tracer
import purerest.logging.Logging
import purerest.metrics.Metrics
import purerest.tracing.{Tracing, TracingConfig}

/** Bundles the three dependencies this service always acquires and threads
  * together — `tracer`, `meter`, `logger` — into one module, per the
  * `Resource`-built-module composition pattern documented in `purerest`'s
  * `docs/functional-design-patterns-scala-haskell-category-theory.md` ("Scaling
  * tagless final without Cake"). Unlike `InventoryClient` (where order-service
  * has only one client today, so a bundling wrapper would be premature),
  * tracer/meter/logger are a *real*, current repetition — every layer of `Main`
  * already receives all three.
  */
final case class Telemetry[F[_]](
    tracer: Tracer[F],
    meter: Meter[F],
    logger: StructuredLogger[F]
)

object Telemetry {

  /** The shared composition logic: given already-constructed tracer/meter
    * `Resource`s, builds the trace-correlated logger (`Logging.create`) and
    * bundles all three. Factored out from `resource` below so it's testable
    * against `Tracing.test`/`Metrics.test` — this codebase's own convention is
    * that `Tracing.resource`/`Metrics.oteljava` (real OpenTelemetry SDK + a
    * bound Prometheus port) are only ever exercised from `Main`, never
    * unit-tested directly.
    */
  def fromResources[F[_]: Sync](
      tracerResource: Resource[F, Tracer[F]],
      meterResource: Resource[F, Meter[F]],
      serviceName: String
  ): Resource[F, Telemetry[F]] =
    for {
      tracer <- tracerResource
      meter <- meterResource
      logger <- Resource.eval(Logging.create[F](tracer, serviceName))
    } yield Telemetry(tracer, meter, logger)

  /** The tracer's exporter and sampling from the standard OTel env vars
    * (`TracingConfig.fromEnv`, PERF-5.3): OTLP/HTTP to Tempo when
    * `OTEL_EXPORTER_OTLP_ENDPOINT` is set, else the console. A malformed
    * `OTEL_TRACES_SAMPLER_ARG` fails startup rather than defaulting.
    */
  def tracingConfig[F[_]: Sync]: F[TracingConfig] =
    Sync[F].defer(
      Sync[F].fromEither(
        TracingConfig.fromEnv(sys.env).left.map(new IllegalArgumentException(_))
      )
    )

  /** Production constructor: a tracer exporting per `tracingConfig`
    * (`Tracing.resource`) and a Prometheus-scrapeable meter
    * (`Metrics.oteljava`, bound to `metricsPort`, with JVM and — given
    * `ioRuntimeMetrics` — cats-effect runtime metrics), composed via
    * `fromResources`.
    */
  def resource[F[_]: Async: LocalContextProvider](
      serviceName: String,
      metricsPort: Int,
      ioRuntimeMetrics: Option[IORuntimeMetrics] = None
  ): Resource[F, Telemetry[F]] =
    fromResources[F](
      Resource
        .eval(tracingConfig[F])
        .flatMap(Tracing.resource[F](serviceName, _)),
      Metrics.oteljava[F](serviceName, metricsPort, ioRuntimeMetrics),
      serviceName
    )
}
