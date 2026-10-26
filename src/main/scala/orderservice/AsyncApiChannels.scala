package orderservice

import purekafka.docs.{
  AsyncApiChannel,
  AsyncApiChannelsProvider,
  ChannelDirection,
  PublisherKind
}

/** Registers order-service's Kafka event contracts for AsyncAPI documentation
  * (ADR 0010's AsyncAPI follow-up; direction-aware since the CDC-direction
  * track in `purerest`), mirroring the real multiplexed topics:
  * `StockEventConsumer`/`PaymentEventConsumer` route two event types each
  * through one topic via the envelope pattern, and are `Subscribe`-only —
  * order-service only ever consumes those four event types, never publishes
  * them (inventory-service/payment-service's own outboxes do).
  *
  * order-service *does* publish — via its own `outbox_events` table and the
  * Debezium CDC connector's `route.topic.replacement` config
  * (`infra/k8s/local-infra/debezium-connectors-register.yaml`, gluon repo), not
  * a direct producer call. `OrderReserved`/`OrderStatusChanged` are registered
  * as `Publish(Cdc)`, correcting the earlier (incorrect) assumption that Rework
  * 4's outbox migration removed every publish-side event this service has — it
  * only removed the *direct* ones.
  *
  * Implements `AsyncApiChannelsProvider` (not just a plain `List`) so the
  * sbt-purekafka-docs plugin's `AsyncApiSpecWriter` can load this singleton
  * reflectively by class name, configured via `build.sbt`'s
  * `asyncApiChannelsClass`.
  */
object AsyncApiChannels extends AsyncApiChannelsProvider {

  // Not read anywhere else in this app's own code — the outbox pattern means
  // order-service never names a Kafka topic directly, only Debezium's
  // connector config does (see this object's own doc comment). Kept here,
  // not as a shared constant, since this registration is this value's only
  // consumer.
  private val orderEventsTopic = "order-events"

  val channels: List[AsyncApiChannel[?]] = List(
    AsyncApiChannel[StockReservedEvent](
      topic = StockEventConsumer.topic,
      eventType = "StockReserved",
      direction = ChannelDirection.Subscribe
    ),
    AsyncApiChannel[StockReservationFailedEvent](
      topic = StockEventConsumer.topic,
      eventType = "StockReservationFailed",
      direction = ChannelDirection.Subscribe
    ),
    AsyncApiChannel[PaymentCapturedEvent](
      topic = PaymentEventConsumer.topic,
      eventType = "PaymentCaptured",
      direction = ChannelDirection.Subscribe
    ),
    AsyncApiChannel[PaymentAuthorizationExpiredEvent](
      topic = PaymentEventConsumer.topic,
      eventType = "PaymentAuthorizationExpired",
      direction = ChannelDirection.Subscribe
    ),
    AsyncApiChannel[PaymentFailedEvent](
      topic = PaymentEventConsumer.topic,
      eventType = "PaymentFailed",
      direction = ChannelDirection.Subscribe
    ),
    AsyncApiChannel[OrderReservedEvent](
      topic = orderEventsTopic,
      eventType = "OrderReserved",
      direction = ChannelDirection.Publish(PublisherKind.Cdc)
    ),
    AsyncApiChannel[OrderStatusChangedEvent](
      topic = orderEventsTopic,
      eventType = "OrderStatusChanged",
      direction = ChannelDirection.Publish(PublisherKind.Cdc)
    )
  )
}
