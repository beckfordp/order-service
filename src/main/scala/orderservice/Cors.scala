package orderservice

import cats.effect.Async
import org.http4s.{HttpRoutes, Method}
import org.http4s.server.middleware.CORS
import org.typelevel.ci._

/** CORS policy for a freshly generated service: allow all origins (no frontend
  * origin decided yet at generation time), restricted to the methods/headers
  * this template's own routes actually use, credentials disallowed (required
  * anyway alongside an allow-all-origins policy — browsers reject credentialed
  * requests against a wildcard origin).
  */
object Cors {

  def middleware[F[_]: Async](routes: HttpRoutes[F]): HttpRoutes[F] =
    CORS.policy.withAllowOriginAll
      .withAllowMethodsIn(
        Set(Method.GET, Method.POST, Method.PATCH, Method.PUT, Method.DELETE)
      )
      .withAllowHeadersIn(Set(ci"Content-Type"))
      .withAllowCredentials(false)
      .apply(routes)
}
