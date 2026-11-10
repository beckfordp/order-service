// SPDX-License-Identifier: MIT

package orderservice

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample}

/** Shared shipping/billing shape pinned in `gluon/docs/system-design.md`'s
  * "REST contracts" (`Address`). order-service uses it as the order's shipping
  * address (US-11.1); payment-service keeps its own copy for billing (no shared
  * library between services).
  */
@description("A postal address.")
final case class Address(
    @description("First address line; required.")
    @encodedExample("1 Example Street")
    line1: String,
    @description("Second address line; optional.") @encodedExample("Flat 2")
    line2: Option[String],
    @description("Town or city; required.") @encodedExample("London")
    city: String,
    @description("Postal code; required.") @encodedExample("EC1A 1AA")
    postalCode: String,
    @description("ISO 3166-1 alpha-2 country code.") @encodedExample("GB")
    country: String
)

object Address {
  implicit val codec: Codec[Address] = deriveCodec
  implicit val schema: Schema[Address] = Schema.derived

  private val alpha2 = "^[A-Z]{2}$".r

  def validate(address: Address): Either[String, Address] =
    for {
      _ <- required("line1", address.line1)
      _ <- required("city", address.city)
      _ <- required("postalCode", address.postalCode)
      _ <- Either.cond(
        alpha2.matches(address.country),
        (),
        "country must be an ISO 3166-1 alpha-2 code"
      )
    } yield address

  private def required(field: String, value: String): Either[String, Unit] =
    Either.cond(value.trim.nonEmpty, (), s"$field is required")
}
