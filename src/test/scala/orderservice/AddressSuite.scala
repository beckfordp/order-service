package orderservice

import io.circe.parser.decode
import io.circe.syntax._
import munit.FunSuite

class AddressSuite extends FunSuite {

  private val valid =
    Address("1 High St", None, "London", "SW1A 1AA", "GB")

  test("validate accepts a complete address with no line2") {
    assertEquals(Address.validate(valid), Right(valid))
  }

  test("validate accepts an address with line2") {
    val withLine2 = valid.copy(line2 = Some("Flat 2"))
    assertEquals(Address.validate(withLine2), Right(withLine2))
  }

  test("validate rejects a blank line1") {
    assertEquals(
      Address.validate(valid.copy(line1 = "  ")),
      Left("line1 is required")
    )
  }

  test("validate rejects a blank city") {
    assertEquals(
      Address.validate(valid.copy(city = "")),
      Left("city is required")
    )
  }

  test("validate rejects a blank postalCode") {
    assertEquals(
      Address.validate(valid.copy(postalCode = " ")),
      Left("postalCode is required")
    )
  }

  test("validate rejects a country that isn't ISO 3166-1 alpha-2 shaped") {
    List("gb", "GBR", "G", "", "G1").foreach { country =>
      assertEquals(
        Address.validate(valid.copy(country = country)),
        Left("country must be an ISO 3166-1 alpha-2 code"),
        country
      )
    }
  }

  test("JSON round-trips with line2 null when absent") {
    val json = valid.asJson
    assertEquals(
      json.hcursor.downField("line2").focus.map(_.isNull),
      Some(true)
    )
    assertEquals(decode[Address](json.noSpaces), Right(valid))
  }
}
