package orderservice

import cats.effect.IO
import munit.CatsEffectSuite
import purerest.metrics.Metrics

import scala.jdk.CollectionConverters._

class CheckoutOutcomesSuite extends CatsEffectSuite {

  test("classifies every checkout result into a low-cardinality outcome") {
    val cases: List[(Either[OrderError, Unit], String)] = List(
      Right(()) -> "authorized",
      Left(PaymentDeclined("card")) -> "declined",
      Left(InsufficientStock(Nil)) -> "out_of_stock",
      Left(PaymentServiceUnavailable) -> "unavailable",
      Left(InventoryServiceUnavailable) -> "unavailable",
      Left(OrderPersistFailed) -> "error",
      Left(InvalidCheckout("bad address")) -> "invalid",
      Left(EmptyOrderItems) -> "invalid",
      Left(InvalidOrderItem("qty")) -> "invalid",
      Left(OrderNotFound) -> "error"
    )
    cases.foreach { (result, expected) =>
      assertEquals(CheckoutOutcomes.classify(result), expected, result)
    }
  }

  test("counts outcomes by label") {
    Metrics.test[IO]("checkout-test").use { tm =>
      for {
        outcomes <- CheckoutOutcomes.create[IO](tm.meter)
        _ <- outcomes.record(Right(()))
        _ <- outcomes.record(Left(PaymentDeclined("x")))
        _ <- outcomes.record(Left(PaymentDeclined("y")))
        _ <- CheckoutOutcomes.noop[IO].record(Right(()))
        metrics <- tm.collectMetrics
      } yield {
        val points = metrics
          .find(_.getName == "checkout.outcome")
          .get
          .getLongSumData
          .getPoints
          .asScala
          .map(p =>
            p.getAttributes.asMap.asScala.values.head.toString -> p.getValue
          )
          .toMap
        assertEquals(points, Map("authorized" -> 1L, "declined" -> 2L))
      }
    }
  }
}
