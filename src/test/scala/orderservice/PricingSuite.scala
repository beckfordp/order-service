package orderservice

import munit.ScalaCheckSuite
import orderservice.domain.Pricing
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

class PricingSuite extends ScalaCheckSuite {

  private val safeLine = for {
    price <- Gen.choose(0, 10_000)
    quantity <- Gen.choose(0, 100)
  } yield Pricing.Line(price, quantity)

  property("totalCents equals the sum of line totals for safe baskets") {
    forAll(Gen.listOf(safeLine)) { lines =>
      val expected =
        lines.map(l => l.unitPriceCents.toLong * l.quantity.toLong).sum
      assertEquals(Pricing.totalCents(lines), Right(expected.toInt))
    }
  }

  property("totalCents rejects a line total that overflows Int") {
    forAll(Gen.choose(Int.MaxValue / 2 + 1, Int.MaxValue)) { price =>
      assert(Pricing.totalCents(List(Pricing.Line(price, 2))).isLeft)
    }
  }

  property("totalCents rejects negative prices or quantities") {
    forAll(Gen.choose(Int.MinValue, -1)) { negative =>
      assert(
        Pricing
          .totalCents(List(Pricing.Line(negative, 1)))
          .isLeft
      )
      assert(
        Pricing
          .totalCents(List(Pricing.Line(1, negative)))
          .isLeft
      )
    }
  }
}
