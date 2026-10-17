package orderservice

import cats.effect.{IO, IOApp}
import cats.effect.syntax.all._
import cats.syntax.semigroupk._
import com.comcast.ip4s._
import org.http4s.Uri
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits._
import org.http4s.server.staticcontent.{FileService, fileService}
import purekafka.docs.{Docs => AsyncApiDocs}
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
                  KafkaConsumers
                    .supervised[IO](config.kafka, store, logger, meter)
                    .compile
                    .drain
                    .background
                    .use { _ =>
                      EmberClientBuilder.default[IO].build.use { httpClient =>
                        OrderHistoryCache
                          .resource[IO](config.redis, logger)
                          .use { historyCache =>
                            val resilientClient =
                              Resilience.middleware[IO](
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
                                OrderRoutes.getOrderServerEndpoint[IO](
                                  store,
                                  logger
                                ),
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
                            val asyncApiRoutes = AsyncApiDocs.routes[IO](
                              "Order Service",
                              "1.0",
                              AsyncApiChannels.all
                            )
                            // Serves scripts/generate-kafka-asyncapi-docs.sh's
                            // output as static HTML at /docs/asyncapi/* —
                            // sibling to asyncApiRoutes' raw-spec route, same
                            // split as Swagger's /docs vs /docs/docs.yaml.
                            // order-service runs as a plain `sbt run` JVM
                            // process (no docker bind mount, unlike
                            // kafka-harness), so this reads straight from the
                            // local filesystem.
                            val staticAsyncApiDocsDir = sys.env.getOrElse(
                              "ASYNCAPI_HTML_DIR",
                              "target/asyncapi-docs"
                            )
                            val staticAsyncApiRoutes = fileService[IO](
                              FileService.Config(
                                systemPath = staticAsyncApiDocsDir,
                                pathPrefix = "/docs/asyncapi"
                              )
                            )
                            // asyncApiRoutes/staticAsyncApiRoutes must come
                            // before docsRoutes: docsRoutes' Swagger UI
                            // bundle serves a wildcard /docs/{resource}
                            // route for its own static assets, which
                            // returns its own real 404 (not a http4s
                            // fallthrough `None`) for any /docs/* path it
                            // doesn't recognize — including
                            // /docs/asyncapi.json — short-circuiting `<+>`
                            // before asyncApiRoutes ever sees the request.
                            val tracedRoutes =
                              ServerTracing.middleware(tracer)(
                                asyncApiRoutes <+> staticAsyncApiRoutes <+> docsRoutes
                              )
                            val meteredRoutes =
                              ServerMetrics.middleware[IO](meter)(
                                tracedRoutes
                              )
                            val routes =
                              Cors.middleware[IO](meteredRoutes)
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
