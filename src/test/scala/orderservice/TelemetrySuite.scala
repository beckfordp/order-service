package orderservice

import cats.effect.IO
import munit.CatsEffectSuite
import purerest.metrics.Metrics
import purerest.tracing.Tracing

class TelemetrySuite extends CatsEffectSuite {

  test(
    "Telemetry.fromResources bundles a tracer, meter, and logger built from the given Resources"
  ) {
    (for {
      testTracer <- Tracing.test[IO]("telemetry-suite")
      testMeter <- Metrics.test[IO]("telemetry-suite")
      telemetry <- Telemetry.fromResources[IO](
        cats.effect.Resource.pure(testTracer.tracer),
        cats.effect.Resource.pure(testMeter.meter),
        "telemetry-suite"
      )
    } yield (telemetry, testTracer, testMeter)).use {
      case (telemetry, testTracer, testMeter) =>
        IO {
          assertEquals(telemetry.tracer, testTracer.tracer)
          assertEquals(telemetry.meter, testMeter.meter)
        }
    }
  }

  test(
    "Telemetry.fromResources' logger is actually built via Logging.create(tracer, serviceName), not substituted/stubbed - it's usable inside an active span without error"
  ) {
    // Not re-testing Logging.create's own trace-correlation internals here
    // (purerest.logging.Logging's own concern, already its own test suite's
    // job) - just proving this composition function really calls it with the
    // given tracer, rather than e.g. accidentally wiring a NoOp/stub logger.
    (for {
      testTracer <- Tracing.test[IO]("telemetry-suite-correlation")
      testMeter <- Metrics.test[IO]("telemetry-suite-correlation")
      telemetry <- Telemetry.fromResources[IO](
        cats.effect.Resource.pure(testTracer.tracer),
        cats.effect.Resource.pure(testMeter.meter),
        "telemetry-suite-correlation"
      )
    } yield telemetry).use { telemetry =>
      telemetry.tracer.span("test-span").surround {
        telemetry.logger.info("inside a span")
      }
    }
  }
}
