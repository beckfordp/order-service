package orderservice

import purekafka.docs.{AsyncApiChannel, AsyncApiChannelsProvider}

/** Registers order-service's Kafka event contracts for AsyncAPI documentation
  * (ADR 0010's AsyncAPI follow-up), mirroring the real multiplexed topics:
  * `StockEventConsumer`/`PaymentEventConsumer` route two event types each
  * through one topic via the envelope pattern. No producer-side registrations —
  * Rework 4's outbox pattern already removed every direct-publish call site
  * from order-service.
  *
  * Implements `AsyncApiChannelsProvider` (not just a plain `List`) so the
  * sbt-purekafka-docs plugin's `AsyncApiSpecWriter` can load this singleton
  * reflectively by class name, configured via `build.sbt`'s
  * `asyncApiChannelsClass`.
  */
object AsyncApiChannels extends AsyncApiChannelsProvider {

  val channels: List[AsyncApiChannel[?]] = List(
    AsyncApiChannel[StockReservedEvent](
      topic = StockEventConsumer.topic,
      eventType = "StockReserved"
    ),
    AsyncApiChannel[StockReservationFailedEvent](
      topic = StockEventConsumer.topic,
      eventType = "StockReservationFailed"
    ),
    AsyncApiChannel[PaymentSettledEvent](
      topic = PaymentEventConsumer.topic,
      eventType = "PaymentSettled"
    ),
    AsyncApiChannel[PaymentFailedEvent](
      topic = PaymentEventConsumer.topic,
      eventType = "PaymentFailed"
    )
  )
}
