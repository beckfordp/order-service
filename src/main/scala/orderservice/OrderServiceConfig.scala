package orderservice

import cats.effect.Sync
import pureconfig.{ConfigReader, ConfigSource}
import purerest.resilience.{CircuitBreakerConfig, ResilienceConfig, RetryConfig}

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

final case class InventoryClientConfig(
    baseUrl: String,
    resilience: ResilienceConfig
) derives ConfigReader

final case class OrderServiceConfig(
    port: Int,
    metricsPort: Int,
    serviceName: String,
    postgres: PostgresConfig,
    inventoryClient: InventoryClientConfig
) derives ConfigReader

object OrderServiceConfig {
  def load[F[_]: Sync]: F[OrderServiceConfig] =
    Sync[F].delay(ConfigSource.default.loadOrThrow[OrderServiceConfig])
}
