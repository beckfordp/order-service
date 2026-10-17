package orderservice

import munit.FunSuite
import purekafka.docs.AsyncApiChannel

class AsyncApiChannelsSuite extends FunSuite {

  test(
    "AsyncApiChannel constructs for each of order-service's 4 registered event types"
  ) {
    val stockReserved =
      AsyncApiChannel[StockReservedEvent](
        topic = StockEventConsumer.topic,
        eventType = "StockReserved"
      )
    val stockReservationFailed =
      AsyncApiChannel[StockReservationFailedEvent](
        topic = StockEventConsumer.topic,
        eventType = "StockReservationFailed"
      )
    val paymentSettled =
      AsyncApiChannel[PaymentSettledEvent](
        topic = PaymentEventConsumer.topic,
        eventType = "PaymentSettled"
      )
    val paymentFailed =
      AsyncApiChannel[PaymentFailedEvent](
        topic = PaymentEventConsumer.topic,
        eventType = "PaymentFailed"
      )

    assertEquals(stockReserved.topic, "inventory-events")
    assertEquals(stockReservationFailed.topic, "inventory-events")
    assertEquals(paymentSettled.topic, "payment-events")
    assertEquals(paymentFailed.topic, "payment-events")
  }

  test(
    "AsyncApiChannels.all registers all 4 event types with correct topic/eventType pairs"
  ) {
    val topicsAndEventTypes =
      AsyncApiChannels.all.map(c => c.topic -> c.eventType).toSet

    assertEquals(
      topicsAndEventTypes,
      Set(
        "inventory-events" -> "StockReserved",
        "inventory-events" -> "StockReservationFailed",
        "payment-events" -> "PaymentSettled",
        "payment-events" -> "PaymentFailed"
      )
    )
  }
}
