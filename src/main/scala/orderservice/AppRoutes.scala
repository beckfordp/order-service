package orderservice

import cats.effect.Async
import cats.syntax.semigroupk._
import fs2.io.file.Files
import org.http4s.HttpRoutes
import org.http4s.server.staticcontent.{FileService, fileService}
import purekafka.docs.{AsyncApiChannel, Docs => AsyncApiDocs}
import purerest.docs.Docs
import purerest.metrics.ServerMetrics
import purerest.tracing.ServerTracing

/** Assembles order-service's full set of `HttpRoutes[F]` — business routes,
  * OpenAPI/Swagger docs, AsyncAPI docs (raw spec + rendered static HTML) — and
  * applies this service's full middleware stack (server tracing, server
  * metrics, CORS), in one call. Extracted out of `Main` per the
  * `Resource`-built-module composition pattern documented in `purerest`'s
  * `docs/functional-design-patterns-scala-haskell-category-theory.md` ("Scaling
  * tagless final without Cake") — `Main` should just acquire dependencies and
  * call this once, not inline ~80 lines of route construction itself.
  */
object AppRoutes {

  def all[F[_]: Async](
      store: OrderStore[F],
      inventoryClient: InventoryClient[F],
      historyCache: OrderHistoryCache[F],
      telemetry: Telemetry[F],
      channels: List[AsyncApiChannel[?]]
  ): HttpRoutes[F] = {
    // Explicit, non-deprecated Files[F] - without it, fileService[F] below
    // falls back to fs2's deprecated Files.implicitForAsync conversion
    // (the original inline Main.scala code never hit this: it ran
    // concretely in IO, where Files.forIO/forLiftIO already resolves
    // non-deprecated).
    implicit val filesForF: Files[F] = Files.forAsync[F]
    val docsRoutes = Docs.routes[F](
      "Order Service",
      "1.0",
      List(
        OrderRoutes.serverEndpoint[F](store, telemetry.logger, inventoryClient),
        OrderRoutes.listOrdersServerEndpoint[F](
          store,
          telemetry.logger,
          historyCache
        ),
        OrderRoutes.getOrderServerEndpoint[F](store, telemetry.logger),
        OrderRoutes.updateOrderServerEndpoint[F](store, telemetry.logger),
        OrderRoutes.replaceOrderServerEndpoint[F](store, telemetry.logger),
        OrderRoutes.deleteOrderServerEndpoint[F](store, telemetry.logger),
        HealthRoutes.healthServerEndpoint[F],
        HealthRoutes.readyServerEndpoint[F](store)
      )
    )
    val asyncApiRoutes =
      AsyncApiDocs.routes[F]("Order Service", "1.0", channels)
    // Serves scripts/generate-kafka-asyncapi-docs.sh's output as static HTML
    // at /docs/asyncapi/* — sibling to asyncApiRoutes' raw-spec route, same
    // split as Swagger's /docs vs /docs/docs.yaml. order-service runs as a
    // plain `sbt run` JVM process (no docker bind mount, unlike
    // kafka-harness), so this reads straight from the local filesystem.
    val staticAsyncApiDocsDir =
      sys.env.getOrElse("ASYNCAPI_HTML_DIR", "target/asyncapi-docs")
    val staticAsyncApiRoutes = fileService[F](
      FileService.Config(
        systemPath = staticAsyncApiDocsDir,
        pathPrefix = "/docs/asyncapi"
      )
    )
    // asyncApiRoutes/staticAsyncApiRoutes must come before docsRoutes:
    // docsRoutes' Swagger UI bundle serves a wildcard /docs/{resource} route
    // for its own static assets, which returns its own real 404 (not a
    // http4s fallthrough `None`) for any /docs/* path it doesn't recognize —
    // including /docs/asyncapi.json — short-circuiting `<+>` before
    // asyncApiRoutes ever sees the request.
    val tracedRoutes = ServerTracing.middleware(telemetry.tracer)(
      asyncApiRoutes <+> staticAsyncApiRoutes <+> docsRoutes
    )
    val meteredRoutes =
      ServerMetrics.middleware[F](telemetry.meter)(tracedRoutes)
    Cors.middleware[F](meteredRoutes)
  }
}
