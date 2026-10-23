package orderservice

import cats.effect.{IO, IOApp, Resource}
import cats.effect.syntax.all._
import com.comcast.ip4s._
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder

/** Acquires every dependency order-service needs, builds the routes, and serves
  * them — a single flat `Resource` for-comprehension, one `<-` per dependency,
  * per the composition pattern documented in `purerest`'s
  * `docs/functional-design-patterns-scala-haskell-category-theory.md` ("Scaling
  * tagless final without Cake: bundling dependencies into `Resource`-built
  * modules"). Dependencies that don't themselves need releasing (config, the
  * port, running migrations) are still acquired via `Resource.eval`, so the
  * whole chain stays one comprehension — no nested `Resource#use` callbacks
  * anywhere in this file.
  */
object Main extends IOApp.Simple {

  val run: IO[Unit] = (for {
    config <- Resource.eval(OrderServiceConfig.load[IO])
    port <- Resource.eval(
      IO.fromOption(Port.fromInt(config.port))(
        new IllegalArgumentException(
          s"Invalid order-service port: ${config.port}"
        )
      )
    )
    _ <- Resource.eval(Migrations.run[IO](config.postgres))
    telemetry <- Telemetry.resource[IO](config.serviceName, config.metricsPort)
    _ <- Resource.eval(
      telemetry.logger.info(
        Map(
          "port" -> config.port.toString,
          "metrics_port" -> config.metricsPort.toString
        )
      )("order-service starting")
    )
    store <- OrderStore.postgres[IO](config.postgres, telemetry.meter)
    _ <- KafkaConsumers
      .supervised[IO](config.kafka, store, telemetry.logger, telemetry.meter)
      .compile
      .drain
      .background
    httpClient <- EmberClientBuilder.default[IO].build
    inventoryClient = InventoryClient.resilient[IO](
      httpClient,
      config.inventoryClient,
      telemetry
    )
    historyCache <- OrderHistoryCache.resource[IO](
      config.redis,
      telemetry.logger
    )
    routes = AppRoutes.all[IO](
      store,
      inventoryClient,
      historyCache,
      telemetry,
      AsyncApiChannels.channels
    )
    _ <- EmberServerBuilder
      .default[IO]
      .withHost(host"0.0.0.0")
      .withPort(port)
      .withHttpApp(routes.orNotFound)
      .build
  } yield ()).useForever
}
