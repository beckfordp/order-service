package orderservice

import io.circe.parser.parse
import munit.FunSuite
import purekafka.docs.{
  AsyncApiChannel,
  AsyncApiDoc,
  AsyncApiSpecWriter,
  ChannelDirection,
  PublisherKind
}

import java.nio.file.Files

class AsyncApiChannelsSuite extends FunSuite {

  test(
    "AsyncApiChannel constructs for each of order-service's 7 registered event types"
  ) {
    val stockReserved =
      AsyncApiChannel[StockReservedEvent](
        topic = StockEventConsumer.topic,
        eventType = "StockReserved",
        direction = ChannelDirection.Subscribe
      )
    val stockReservationFailed =
      AsyncApiChannel[StockReservationFailedEvent](
        topic = StockEventConsumer.topic,
        eventType = "StockReservationFailed",
        direction = ChannelDirection.Subscribe
      )
    val paymentCaptured =
      AsyncApiChannel[PaymentCapturedEvent](
        topic = PaymentEventConsumer.topic,
        eventType = "PaymentCaptured",
        direction = ChannelDirection.Subscribe
      )
    val paymentAuthorizationExpired =
      AsyncApiChannel[PaymentAuthorizationExpiredEvent](
        topic = PaymentEventConsumer.topic,
        eventType = "PaymentAuthorizationExpired",
        direction = ChannelDirection.Subscribe
      )
    val paymentFailed =
      AsyncApiChannel[PaymentFailedEvent](
        topic = PaymentEventConsumer.topic,
        eventType = "PaymentFailed",
        direction = ChannelDirection.Subscribe
      )
    val orderReserved =
      AsyncApiChannel[OrderReservedEvent](
        topic = "order-events",
        eventType = "OrderReserved",
        direction = ChannelDirection.Publish(PublisherKind.Cdc)
      )
    val orderStatusChanged =
      AsyncApiChannel[OrderStatusChangedEvent](
        topic = "order-events",
        eventType = "OrderStatusChanged",
        direction = ChannelDirection.Publish(PublisherKind.Cdc)
      )

    assertEquals(stockReserved.topic, "inventory-events")
    assertEquals(stockReservationFailed.topic, "inventory-events")
    assertEquals(paymentCaptured.topic, "payment-events")
    assertEquals(paymentAuthorizationExpired.topic, "payment-events")
    assertEquals(paymentFailed.topic, "payment-events")
    assertEquals(orderReserved.topic, "order-events")
    assertEquals(orderStatusChanged.topic, "order-events")
  }

  test(
    "AsyncApiChannels.channels registers all 7 event types with correct topic/eventType pairs"
  ) {
    val topicsAndEventTypes =
      AsyncApiChannels.channels.map(c => c.topic -> c.eventType).toSet

    assertEquals(
      topicsAndEventTypes,
      Set(
        "inventory-events" -> "StockReserved",
        "inventory-events" -> "StockReservationFailed",
        "payment-events" -> "PaymentCaptured",
        "payment-events" -> "PaymentAuthorizationExpired",
        "payment-events" -> "PaymentFailed",
        "order-events" -> "OrderReserved",
        "order-events" -> "OrderStatusChanged"
      )
    )
  }

  test(
    "the 5 consumed event types are Subscribe-only; the 2 CDC-published event types are Publish(Cdc)"
  ) {
    val directionsByEventType =
      AsyncApiChannels.channels.map(c => c.eventType -> c.direction).toMap

    for (
      consumed <- List(
        "StockReserved",
        "StockReservationFailed",
        "PaymentCaptured",
        "PaymentAuthorizationExpired",
        "PaymentFailed"
      )
    )
      assertEquals(
        directionsByEventType(consumed),
        ChannelDirection.Subscribe: ChannelDirection,
        s"expected $consumed to be Subscribe-only - order-service only consumes it"
      )

    for (published <- List("OrderReserved", "OrderStatusChanged"))
      assertEquals(
        directionsByEventType(published),
        ChannelDirection.Publish(PublisherKind.Cdc): ChannelDirection,
        s"expected $published to be Publish(Cdc) - order-service's own outbox publishes it via Debezium CDC"
      )
  }

  test(
    "orderservice.AsyncApiChannels is reflectively loadable by name, matching build.sbt's asyncApiChannelsClass setting - a regression guard for sbt-purekafka-docs' AsyncApiSpecWriter"
  ) {
    val outputPath =
      Files.createTempFile("order-service-asyncapi-test", ".json")
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
