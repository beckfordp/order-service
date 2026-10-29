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
    "AsyncApiChannel constructs for each of order-service's 3 order-events publish event types"
  ) {
    val orderConfirmed =
      AsyncApiChannel[OrderConfirmedEvent](
        topic = "order-events",
        eventType = "OrderConfirmed",
        direction = ChannelDirection.Publish(PublisherKind.Cdc)
      )
    val orderPaymentFailed =
      AsyncApiChannel[OrderPaymentFailedEvent](
        topic = "order-events",
        eventType = "OrderPaymentFailed",
        direction = ChannelDirection.Publish(PublisherKind.Cdc)
      )
    val orderCancelled =
      AsyncApiChannel[OrderCancelledEvent](
        topic = "order-events",
        eventType = "OrderCancelled",
        direction = ChannelDirection.Publish(PublisherKind.Cdc)
      )

    assertEquals(orderConfirmed.topic, "order-events")
    assertEquals(orderPaymentFailed.topic, "order-events")
    assertEquals(orderCancelled.topic, "order-events")
  }

  test(
    "AsyncApiChannels.channels no longer registers the retired stock/OrderReserved/OrderStatusChanged events"
  ) {
    val eventTypes = AsyncApiChannels.channels.map(_.eventType).toSet
    for (
      retired <- List(
        "StockReserved",
        "StockReservationFailed",
        "OrderReserved",
        "OrderStatusChanged"
      )
    )
      assert(
        !eventTypes.contains(retired),
        s"$retired should be retired (ADR 0018, TD-7.3, TD-15)"
      )
  }

  test(
    "AsyncApiChannels.channels registers all 8 event types with correct topic/eventType pairs"
  ) {
    val topicsAndEventTypes =
      AsyncApiChannels.channels.map(c => c.topic -> c.eventType).toSet

    assertEquals(
      topicsAndEventTypes,
      Set(
        "payment-events" -> "PaymentCaptured",
        "payment-events" -> "PaymentAuthorizationExpired",
        "payment-events" -> "PaymentFailed",
        "order-events" -> "OrderConfirmed",
        "order-events" -> "OrderPaymentFailed",
        "order-events" -> "OrderCancelled",
        "fulfilment-events" -> "DispatchConfirmed",
        "fulfilment-events" -> "DispatchFailed"
      )
    )
  }

  test(
    "the 5 consumed event types are Subscribe-only; all 3 order-events events are Publish(Cdc)"
  ) {
    val directionsByEventType =
      AsyncApiChannels.channels.map(c => c.eventType -> c.direction).toMap

    for (
      consumed <- List(
        "PaymentCaptured",
        "PaymentAuthorizationExpired",
        "PaymentFailed",
        "DispatchConfirmed",
        "DispatchFailed"
      )
    )
      assertEquals(
        directionsByEventType(consumed),
        ChannelDirection.Subscribe: ChannelDirection,
        s"expected $consumed to be Subscribe-only - order-service only consumes it"
      )

    for (
      published <- List("OrderConfirmed", "OrderPaymentFailed", "OrderCancelled")
    )
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
