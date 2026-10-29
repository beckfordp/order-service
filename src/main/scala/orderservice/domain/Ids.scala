package orderservice.domain

import cats.syntax.all._
import sttp.tapir.{Codec, CodecFormat, DecodeResult, Schema}

import java.util.UUID

/** Opaque order identifier. Raw HTTP/event input is parsed exactly once, at the
  * boundary, into this type (parse, don't validate - scala-fp.md law 5); inner
  * layers never see a raw String or a `Try(...).toOption` collapse.
  */
opaque type OrderId = UUID

object OrderId {

  def parse(raw: String): Either[String, OrderId] =
    Either
      .catchOnly[IllegalArgumentException](UUID.fromString(raw))
      .leftMap(_ => s"Invalid order id: '$raw'")

  def fromUuid(value: UUID): OrderId = value

  def uuid(value: OrderId): UUID = value

  def render(value: OrderId): String = value.toString

  given Codec[String, OrderId, CodecFormat.TextPlain] =
    Codec.string.mapDecode(raw =>
      parse(raw) match {
        case Right(id) => DecodeResult.Value(id)
        case Left(msg) =>
          DecodeResult.Error(raw, new IllegalArgumentException(msg))
      }
    )(render)

  given Schema[OrderId] = Schema.string[OrderId]
}

/** Opaque line-item identifier, minted by order-service itself. Kept distinct
  * from [[OrderId]] so the two can never be passed for one another.
  */
opaque type OrderItemId = UUID

object OrderItemId {

  def parse(raw: String): Either[String, OrderItemId] =
    Either
      .catchOnly[IllegalArgumentException](UUID.fromString(raw))
      .leftMap(_ => s"Invalid order item id: '$raw'")

  def fromUuid(value: UUID): OrderItemId = value

  def uuid(value: OrderItemId): UUID = value

  def render(value: OrderItemId): String = value.toString
}
