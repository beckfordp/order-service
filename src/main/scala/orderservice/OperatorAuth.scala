package orderservice

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/** Dev/demo operator gate (US-22, ADR 0024).
  *
  * A request with no `Authorization` header is a customer cancellation, exactly
  * as before. A `Bearer` token matching the configured value selects the
  * operator actor; anything else is rejected with 401. The configured token is
  * a local/demo shortcut, not a real authorization boundary.
  */
final case class OperatorAuth(token: Option[String], operatorId: String) {

  def authenticate(
      authorization: Option[String]
  ): Either[OrderError, Option[String]] =
    authorization match {
      case None      => Right(None)
      case Some(raw) =>
        token match {
          case Some(expected) if bearerMatches(raw, expected) =>
            Right(Some(operatorId))
          case _ => Left(Unauthorized)
        }
    }

  private def bearerMatches(raw: String, expected: String): Boolean = {
    val scheme = "bearer "
    if (raw.length <= scheme.length) false
    else if (!raw.substring(0, scheme.length).equalsIgnoreCase(scheme)) false
    else {
      val presented = raw.substring(scheme.length).trim
      MessageDigest.isEqual(
        presented.getBytes(StandardCharsets.UTF_8),
        expected.getBytes(StandardCharsets.UTF_8)
      )
    }
  }
}

object OperatorAuth {
  val disabled: OperatorAuth = OperatorAuth(None, "dev-operator")

  def fromConfig(config: OperatorAuthConfig): OperatorAuth =
    OperatorAuth(
      config.token.map(_.trim).filter(_.nonEmpty),
      config.operatorId.trim
    )
}
