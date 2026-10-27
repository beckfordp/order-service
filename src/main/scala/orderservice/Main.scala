package orderservice

import cats.effect.{IO, IOApp, Resource}
import com.comcast.ip4s._
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder

/** Acquires every dependency order-service needs, builds the routes, and serves
  * them — a single flat `Resource` for-comprehension, one `<-` per dependency,
  * per the composition pattern documented in `purerest`'s
  * `docs/functional-design-patterns-scala-haskell-category-theory.md` ("Scaling
  * tagless final without Cake: bundling dependencies into `Resource`-built
  * modules"). Every dependency that isn't already a `Resource` is acquired via
  * a `.resource`/`.loadValidated` method on its own owning type
  * (`OrderServiceConfig`, `Migrations`, `KafkaConsumers`, `Telemetry`,
  * `OrderHistoryCache`), so none of their internals (PureConfig, Flyway, fs2
  * streaming) leak into this file. The two exceptions: `EmberClientBuilder`/
  * `EmberServerBuilder` are already `Resource`-returning (http4s' own types, no
  * wrapper needed), and the startup log line stays an explicit `Resource.eval`
  * here since logging isn't itself a dependency.
  */
object Main extends IOApp.Simple {

  val run: IO[Unit] = (for {
    (config, port) <- Resource.eval(OrderServiceConfig.loadValidated[IO])
    _ <- Migrations.resource[IO](config.postgres)
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
    _ <- KafkaConsumers.resource[IO](
      config.kafka,
      store,
      telemetry.logger,
      telemetry.meter
    )
    httpClient <- EmberClientBuilder.default[IO].build
    inventoryClient = InventoryClient.resilient[IO](
      httpClient,
      config.inventoryClient,
      telemetry
    )
    paymentClient = PaymentClient.resilient[IO](
      httpClient,
      config.paymentClient,
      telemetry
    )
    historyCache <- OrderHistoryCache.resource[IO](
      config.redis,
      telemetry.logger
    )
    routes = AppRoutes.all[IO](
      store,
      inventoryClient,
      paymentClient,
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
