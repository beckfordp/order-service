package orderservice

import cats.effect.IO
import munit.CatsEffectSuite
import pureconfig.ConfigSource
import purerest.resilience.{CircuitBreakerConfig, ResilienceConfig, RetryConfig}

import scala.concurrent.duration._

class OrderServiceConfigSuite extends CatsEffectSuite {

  private val validHocon =
    """
      |port = 8080
      |metrics-port = 9090
      |service-name = "order-service"
      |postgres {
      |  host = "localhost"
      |  port = 5432
      |  database = "order"
      |  user = "order"
      |  password = "order"
      |}
      |inventory-client {
      |  base-url = "http://localhost:8081"
      |  resilience {
      |    retry {
      |      max-retries = 3
      |      base-delay = 100ms
      |    }
      |    circuit-breaker {
      |      failure-threshold = 5
      |      reset-timeout = 30s
      |    }
      |  }
      |}
      |kafka {
      |  bootstrap-servers = "localhost:9092"
      |}
      |redis {
      |  uri = "redis://localhost:6379"
      |  history-ttl-seconds = 60
      |}
      |""".stripMargin

  test("loads a fully-specified config") {
    val result =
      ConfigSource.string(validHocon).load[OrderServiceConfig]
    assertEquals(
      result,
      Right(
        OrderServiceConfig(
          port = 8080,
          metricsPort = 9090,
          serviceName = "order-service",
          postgres = PostgresConfig(
            host = "localhost",
            port = 5432,
            database = "order",
            user = "order",
            password = "order"
          ),
          inventoryClient = InventoryClientConfig(
            baseUrl = "http://localhost:8081",
            resilience = ResilienceConfig(
              retry = RetryConfig(maxRetries = 3, baseDelay = 100.millis),
              circuitBreaker = CircuitBreakerConfig(
                failureThreshold = 5,
                resetTimeout = 30.seconds
              )
            )
          ),
          kafka = KafkaConfig(bootstrapServers = "localhost:9092"),
          redis = RedisConfig(
            uri = "redis://localhost:6379",
            historyTtlSeconds = 60
          )
        )
      )
    )
  }

  test("fails to load when a required field is missing") {
    val missingPassword =
      """
        |port = 8080
        |metrics-port = 9090
        |postgres {
        |  host = "localhost"
        |  port = 5432
        |  database = "order"
        |  user = "order"
        |}
        |inventory-client {
        |  base-url = "http://localhost:8081"
        |  resilience {
        |    retry {
        |      max-retries = 3
        |      base-delay = 100ms
        |    }
        |    circuit-breaker {
        |      failure-threshold = 5
        |      reset-timeout = 30s
        |    }
        |  }
        |}
        |""".stripMargin

    assert(
      ConfigSource
        .string(missingPassword)
        .load[OrderServiceConfig]
        .isLeft
    )
  }

  test("load[F] reads the shipped application.conf defaults") {
    OrderServiceConfig.load[IO].map { config =>
      assertEquals(config.port, 8080)
      assertEquals(config.metricsPort, 9090)
      assertEquals(config.serviceName, "order-service")
      assertEquals(
        config.postgres,
        PostgresConfig(
          "localhost",
          5432,
          "order",
          "order",
          "order"
        )
      )
      assertEquals(
        config.inventoryClient,
        InventoryClientConfig(
          baseUrl = "http://localhost:8081",
          resilience = ResilienceConfig(
            retry = RetryConfig(maxRetries = 3, baseDelay = 100.millis),
            circuitBreaker =
              CircuitBreakerConfig(failureThreshold = 5, resetTimeout = 30.seconds)
          )
        )
      )
      assertEquals(
        config.kafka,
        KafkaConfig(bootstrapServers = "localhost:9092")
      )
      assertEquals(
        config.redis,
        RedisConfig(uri = "redis://localhost:6379", historyTtlSeconds = 60)
      )
    }
  }
}
