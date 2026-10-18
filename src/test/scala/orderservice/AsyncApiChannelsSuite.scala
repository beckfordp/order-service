package orderservice

import io.circe.parser.parse
import munit.FunSuite
import purekafka.docs.{AsyncApiChannel, AsyncApiDoc, AsyncApiSpecWriter}

import java.nio.file.Files

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
    "AsyncApiChannels.channels registers all 4 event types with correct topic/eventType pairs"
  ) {
    val topicsAndEventTypes =
      AsyncApiChannels.channels.map(c => c.topic -> c.eventType).toSet

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

  test(
    "orderservice.AsyncApiChannels is reflectively loadable by name, matching build.sbt's asyncApiChannelsClass setting - a regression guard for sbt-purekafka-docs' AsyncApiSpecWriter"
  ) {
    val outputPath = Files.createTempFile("order-service-asyncapi-test", ".json")
    outputPath.toFile.deleteOnExit()

    AsyncApiSpecWriter.main(
      Array(
        "orderservice.AsyncApiChannels",
        "Order Service",
        "1.0",
        outputPath.toString
      )
    )

    val written = parse(Files.readString(outputPath)).toOption.get
    val expected =
      AsyncApiDoc.generate("Order Service", "1.0", AsyncApiChannels.channels)

    assertEquals(written, expected)
  }
}
