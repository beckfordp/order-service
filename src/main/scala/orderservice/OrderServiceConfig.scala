package orderservice

import cats.effect.Sync
import cats.syntax.all._
import com.comcast.ip4s.Port
import pureconfig.{ConfigReader, ConfigSource}
import purekafka.consumer.StreamSupervisorConfig
import purekafka.resilience.{
  CircuitBreakerConfig => KafkaCircuitBreakerConfig,
  ResilienceConfig => KafkaResilienceConfig,
  RetryConfig => KafkaRetryConfig
}
import purerest.resilience.{CircuitBreakerConfig, ResilienceConfig, RetryConfig}

import scala.concurrent.duration._

final case class PostgresConfig(
    host: String,
    port: Int,
    database: String,
    user: String,
    password: String
) derives ConfigReader

// purerest's resilience case classes don't derive ConfigReader themselves
// (the library has no PureConfig dependency) - instances are derived here,
// the same way ClientResilienceExampleSuite derives its own copies.
given ConfigReader[RetryConfig] = ConfigReader.derived
given ConfigReader[CircuitBreakerConfig] = ConfigReader.derived
given ConfigReader[ResilienceConfig] = ConfigReader.derived

// purekafka's own resilience/consumer config case classes - same reasoning
// as above, distinct given instances since these are different types from
// purerest's identically-named ones.
given ConfigReader[KafkaRetryConfig] = ConfigReader.derived
given ConfigReader[KafkaCircuitBreakerConfig] = ConfigReader.derived
given ConfigReader[KafkaResilienceConfig] = ConfigReader.derived
given ConfigReader[StreamSupervisorConfig] = ConfigReader.derived

final case class InventoryClientConfig(
    baseUrl: String,
    resilience: ResilienceConfig
) derives ConfigReader

// Defaults below are for direct Scala construction only (e.g. test helpers
// that build a KafkaConfig without caring about these three fields) -
// confirmed empirically that this version's ConfigReader.derived does *not*
// fall back to them when a key is missing from HOCON (KeyNotFound instead),
// so application.conf always specifies all three explicitly.
final case class KafkaConfig(
    bootstrapServers: String,
    // Message-processing retry before a message is routed to the DLQ - see
    // KafkaConsumers.scala's DeadLetter usage.
    dlqRetry: KafkaRetryConfig =
      KafkaRetryConfig(maxRetries = 3, baseDelay = 200.millis),
    // Guards the DLQ's own publish call (previously unprotected - see
    // purekafka's DeadLetter.scala). Defaults match inventory-client's own
    // resilience numbers above, for consistency within this file.
    dlqPublishResilience: KafkaResilienceConfig = KafkaResilienceConfig(
      retry = KafkaRetryConfig(maxRetries = 3, baseDelay = 100.millis),
      circuitBreaker = KafkaCircuitBreakerConfig(
        failureThreshold = 5,
        resetTimeout = 30.seconds
      )
    ),
    // Restart-with-backoff for a crashed consumer stream (TD-2.2, ADR 0010) -
    // see KafkaConsumers.scala's StreamSupervisor usage.
    streamSupervisor: StreamSupervisorConfig =
      StreamSupervisorConfig(baseDelay = 1.second, maxDelay = 30.seconds)
) derives ConfigReader

final case class RedisConfig(
    uri: String,
    historyTtlSeconds: Int
) derives ConfigReader

final case class OrderServiceConfig(
    port: Int,
    metricsPort: Int,
    serviceName: String,
    postgres: PostgresConfig,
    inventoryClient: InventoryClientConfig,
    kafka: KafkaConfig,
    redis: RedisConfig
) derives ConfigReader

object OrderServiceConfig {
  def load[F[_]: Sync]: F[OrderServiceConfig] =
    Sync[F].delay(ConfigSource.default.loadOrThrow[OrderServiceConfig])

  /** `load`, plus parsing/validating `port` as an `ip4s.Port` - so `Main`
    * doesn't have to do that validation itself.
    */
  def loadValidated[F[_]: Sync]: F[(OrderServiceConfig, Port)] =
    for {
      config <- load[F]
      port <- Sync[F].fromOption(
        Port.fromInt(config.port),
        new IllegalArgumentException(
          s"Invalid order-service port: ${config.port}"
        )
      )
    } yield (config, port)
}
