package orderservice

import munit.FunSuite

class OperatorAuthSuite extends FunSuite {

  test("no Authorization header is a customer cancellation") {
    assertEquals(OperatorAuth.disabled.authenticate(None), Right(None))
    assertEquals(
      OperatorAuth(Some("secret"), "op-1").authenticate(None),
      Right(None)
    )
  }

  test("the configured Bearer token selects the operator") {
    val auth = OperatorAuth(Some("secret"), "op-1")
    assertEquals(auth.authenticate(Some("Bearer secret")), Right(Some("op-1")))
    assertEquals(
      auth.authenticate(Some("bearer secret")),
      Right(Some("op-1"))
    )
  }

  test("malformed or mismatched tokens are rejected") {
    val auth = OperatorAuth(Some("secret"), "op-1")
    List(
      Some("Bearer wrong"),
      Some("Basic secret"),
      Some("Bearer "),
      Some("secret")
    ).foreach(header =>
      assertEquals(auth.authenticate(header), Left(Unauthorized))
    )
  }

  test("a blank configured token disables operator acceptance") {
    assertEquals(
      OperatorAuth.fromConfig(OperatorAuthConfig(Some("  "), "op-1")),
      OperatorAuth(None, "op-1")
    )
    assertEquals(
      OperatorAuth
        .fromConfig(OperatorAuthConfig(Some("  "), "op-1"))
        .authenticate(Some("Bearer anything")),
      Left(Unauthorized)
    )
  }
}
