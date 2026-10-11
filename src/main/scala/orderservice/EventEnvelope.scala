package orderservice

import io.circe.{Codec, Json}
import io.circe.generic.semiauto.deriveCodec

import java.time.Instant

/** Shared Kafka message envelope — per ADR 0009 (`gluon/docs/adr/0009-...`),
  * one topic per domain, every event type on that topic distinguished by
  * `eventType` instead of a dedicated topic. No shared library between services
  * (matching this codebase's existing pattern for payload shapes) — each
  * service keeps its own identical copy of this shape.
  *
  * `payload` is kept as raw `Json` rather than a type parameter: a consumer
  * must read `eventType` *before* it knows which case class to decode `payload`
  * into, so a single parametrized `Codec[EventEnvelope[A]]` wouldn't fit how
  * this is actually used on the read side.
  */
final case class EventEnvelope(
    eventId: String,
    eventType: String,
    timestamp: Instant,
    source: String,
    correlationId: String,
    payload: Json
)

object EventEnvelope {
  implicit val codec: Codec[EventEnvelope] = deriveCodec
}
