package orderservice

import cats.effect.Sync
import pureconfig.{ConfigReader, ConfigSource}

final case class PostgresConfig(
    host: String,
    port: Int,
    database: String,
    user: String,
    password: String
) derives ConfigReader

final case class OrderServiceConfig(
    port: Int,
    metricsPort: Int,
    serviceName: String,
    postgres: PostgresConfig
) derives ConfigReader

object OrderServiceConfig {
  def load[F[_]: Sync]: F[OrderServiceConfig] =
    Sync[F].delay(ConfigSource.default.loadOrThrow[OrderServiceConfig])
}
