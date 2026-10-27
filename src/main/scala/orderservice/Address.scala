package orderservice

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema

/** Shared shipping/billing shape pinned in `gluon/docs/system-design.md`'s
  * "REST contracts" (`Address`). order-service uses it as the order's shipping
  * address (US-11.1); payment-service keeps its own copy for billing (no shared
  * library between services).
  */
final case class Address(
    line1: String,
    line2: Option[String],
    city: String,
    postalCode: String,
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
