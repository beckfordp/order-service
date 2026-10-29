package orderservice.domain

import cats.syntax.all._

/** Pure order pricing, kept out of the effectful shell and computed once per
  * checkout (scala-fp.md laws 1 and 7). Totals are summed in `Long` and only
  * narrowed to the wire contract's `Int` after an explicit range check, so a
  * hostile or accidental large basket can never wrap around.
  */
object Pricing {

  final case class Line(unitPriceCents: Int, quantity: Int)

  def totalCents(lines: List[Line]): Either[String, Int] =
    lines
      .foldLeft[Either[String, Long]](Right(0L)) { (acc, line) =>
        for {
          running <- acc
          _ <- Either.cond(
            line.unitPriceCents >= 0,
            (),
            s"unitPriceCents must be non-negative, got ${line.unitPriceCents}"
          )
          _ <- Either.cond(
            line.quantity >= 0,
            (),
            s"quantity must be non-negative, got ${line.quantity}"
          )
          lineTotal <- checkedMultiply(
            line.unitPriceCents.toLong,
            line.quantity.toLong
          )
          next <- checkedAdd(running, lineTotal)
        } yield next
      }
      .flatMap(toInt)

  private def checkedMultiply(a: Long, b: Long): Either[String, Long] =
    Either
      .catchOnly[ArithmeticException](Math.multiplyExact(a, b))
      .leftMap(_ => s"order line total overflows Long: $a * $b")

  private def checkedAdd(a: Long, b: Long): Either[String, Long] =
    Either
      .catchOnly[ArithmeticException](Math.addExact(a, b))
      .leftMap(_ => s"order total overflows Long: $a + $b")

  private def toInt(value: Long): Either[String, Int] =
    if (value >= 0L && value <= Int.MaxValue.toLong) Right(value.toInt)
    else Left(s"order total exceeds the wire contract's Int range: $value")
}
