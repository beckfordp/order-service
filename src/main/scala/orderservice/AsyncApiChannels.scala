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
  * `PaymentEventConsumer` routes three event types through one topic via the
  * envelope pattern, `Subscribe`-only — order-service consumes them, never
  * publishes them (payment-service's own outbox does). Stock events are no
  * longer consumed: checkout reserves synchronously (ADR 0018).
  *
  * order-service *does* publish — via its own `outbox_events` table and the
  * Debezium CDC connector's `route.topic.replacement` config
  * (`infra/k8s/local-infra/debezium-connectors-register.yaml`, gluon repo), not
  * a direct producer call. `OrderStatusChanged` is registered as
  * `Publish(Cdc)`; `OrderReserved` was retired with ADR 0018.
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
    AsyncApiChannel[OrderStatusChangedEvent](
      topic = orderEventsTopic,
      eventType = "OrderStatusChanged",
      direction = ChannelDirection.Publish(PublisherKind.Cdc)
    ),
    AsyncApiChannel[DispatchConfirmedEvent](
      topic = "fulfilment-events",
      eventType = "DispatchConfirmed",
      direction = ChannelDirection.Subscribe
    )
  )
}
