package orderservice.domain

import munit.FunSuite

class GuestEmailSuite extends FunSuite {

  test("absent or blank means no guest email") {
    assertEquals(GuestEmail.normalize(None), Right(None))
    assertEquals(GuestEmail.normalize(Some("   ")), Right(None))
  }

  test("a valid address is trimmed and lower-cased") {
    assertEquals(
      GuestEmail.normalize(Some("  Guest@Example.COM ")),
      Right(Some("guest@example.com"))
    )
  }

  test("a malformed address is rejected") {
    List("guest", "guest@", "@example.com", "a b@example.com", "guest@example")
      .foreach { raw =>
        assert(GuestEmail.normalize(Some(raw)).isLeft, clue(raw))
      }
  }
}
