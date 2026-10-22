package orderservice

import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.{Header, HttpRoutes, Method, Request}
import org.http4s.dsl.io._
import org.http4s.implicits._
import org.typelevel.ci._

class CorsSuite extends CatsEffectSuite {

  private val dummyRoutes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "orders"  => Ok("[]")
    case POST -> Root / "orders" => Created("{}")
  }

  private val corsRoutes = Cors.middleware[IO](dummyRoutes)

  test("a cross-origin GET receives Access-Control-Allow-Origin") {
    val request = Request[IO](Method.GET, uri"/orders")
      .putHeaders(Header.Raw(ci"Origin", "http://example.com"))
    for {
      response <- corsRoutes.orNotFound.run(request)
    } yield assert(
      response.headers.get(ci"Access-Control-Allow-Origin").isDefined
    )
  }

  test(
    "an OPTIONS preflight for POST receives the expected allow-methods/headers"
  ) {
    val request = Request[IO](Method.OPTIONS, uri"/orders")
      .putHeaders(
        Header.Raw(ci"Origin", "http://example.com"),
        Header.Raw(ci"Access-Control-Request-Method", "POST")
      )
    for {
      response <- corsRoutes.orNotFound.run(request)
      allowMethods = response.headers.get(ci"Access-Control-Allow-Methods")
      allowHeaders = response.headers.get(ci"Access-Control-Allow-Headers")
    } yield {
      assert(response.status.isSuccess)
      assert(allowMethods.exists(_.head.value.contains("POST")))
      assert(
        allowHeaders.exists(_.head.value.toLowerCase.contains("content-type"))
      )
    }
  }

  test("no Access-Control-Allow-Credentials header is sent") {
    val request = Request[IO](Method.GET, uri"/orders")
      .putHeaders(Header.Raw(ci"Origin", "http://example.com"))
    for {
      response <- corsRoutes.orNotFound.run(request)
    } yield assertEquals(
      response.headers.get(ci"Access-Control-Allow-Credentials"),
      None
    )
  }
}
