package orderservice

import cats.effect.{IO, IOApp}
import cats.effect.syntax.all._
import com.comcast.ip4s._
import org.http4s.Uri
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits._
import purerest.docs.Docs
import purerest.logging.Logging
import purerest.metrics.{Metrics, ServerMetrics}
import purerest.resilience.Resilience
import purerest.tracing.{ServerTracing, Tracing}

object Main extends IOApp.Simple {

  val run: IO[Unit] =
    for {
      config <- OrderServiceConfig.load[IO]
      port <- IO.fromOption(Port.fromInt(config.port))(
        new IllegalArgumentException(
          s"Invalid order-service port: ${config.port}"
        )
      )
      _ <- Migrations.run[IO](config.postgres)
      _ <- Tracing.console[IO](config.serviceName).use { tracer =>
        Metrics.oteljava[IO](config.serviceName, config.metricsPort).use {
          meter =>
            for {
              logger <- Logging.create[IO](tracer, config.serviceName)
              _ <- logger.info(
                Map(
                  "port" -> config.port.toString,
                  "metrics_port" -> config.metricsPort.toString
                )
              )("order-service starting")
              _ <- OrderStore.postgres[IO](config.postgres, meter).use {
                store =>
                  StockEventConsumer
                    .run[IO](config.kafka, store, logger)
                    .compile
                    .drain
                    .background
                    .use { _ =>
                      EmberClientBuilder.default[IO].build.use { httpClient =>
                        OrderHistoryCache
                          .resource[IO](config.redis, logger)
                          .use { historyCache =>
                            val resilientClient = Resilience.middleware[IO](
                              config.inventoryClient.resilience
                            )(logger)(meter)(httpClient)
                            val inventoryClient = InventoryClient[IO](
                              resilientClient,
                              Uri.unsafeFromString(
                                config.inventoryClient.baseUrl
                              )
                            )
                            val docsRoutes = Docs.routes[IO](
                              "Order Service",
                              "1.0",
                              List(
                                OrderRoutes.serverEndpoint[IO](
                                  store,
                                  logger,
                                  inventoryClient
                                ),
                                OrderRoutes.listOrdersServerEndpoint[IO](
                                  store,
                                  logger,
                                  historyCache
                                ),
                                OrderRoutes
                                  .getOrderServerEndpoint[IO](store, logger),
                                OrderRoutes.updateOrderServerEndpoint[IO](
                                  store,
                                  logger
                                ),
                                OrderRoutes.replaceOrderServerEndpoint[IO](
                                  store,
                                  logger
                                ),
                                OrderRoutes.deleteOrderServerEndpoint[IO](
                                  store,
                                  logger
                                ),
                                HealthRoutes.healthServerEndpoint[IO],
                                HealthRoutes.readyServerEndpoint[IO](store)
                              )
                            )
                            val tracedRoutes =
                              ServerTracing.middleware(tracer)(docsRoutes)
                            val routes =
                              ServerMetrics.middleware[IO](meter)(
                                tracedRoutes
                              )
                            EmberServerBuilder
                              .default[IO]
                              .withHost(host"0.0.0.0")
                              .withPort(port)
                              .withHttpApp(routes.orNotFound)
                              .build
                              .useForever
                          }
                      }
                    }
              }
            } yield ()
        }
      }
    } yield ()
}
