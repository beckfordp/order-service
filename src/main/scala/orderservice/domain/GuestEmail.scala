// SPDX-License-Identifier: MIT

package orderservice.domain

/** ADM-10: the optional checkout email for a guest (no user-service account).
  * Blank means absent; a present value is trimmed, lower-cased and must look
  * like `local@domain.tld` - the same normalization user-service applies at
  * signup, so both sources compare equal.
  */
object GuestEmail {

  private val shape = "^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$".r

  def normalize(raw: Option[String]): Either[String, Option[String]] =
    raw.map(_.trim).filter(_.nonEmpty) match {
      case None        => Right(None)
      case Some(value) =>
        val normalized = value.toLowerCase(java.util.Locale.ROOT)
        Either.cond(
          shape.matches(normalized) && normalized.length <= 254,
          Some(normalized),
          "email must be a valid email address"
        )
    }
}
