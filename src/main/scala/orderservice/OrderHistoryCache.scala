package orderservice

import cats.effect.{Async, Ref, Resource, Sync}
import cats.syntax.all._
import dev.profunktor.redis4cats.Redis
import dev.profunktor.redis4cats.RedisCommands
import dev.profunktor.redis4cats.effect.{Log, MkRedis}
import dev.profunktor.redis4cats.log4cats.log4CatsInstance
import io.circe.parser.decode
import io.circe.syntax._
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.duration.FiniteDuration

/** Cache-aside read-through cache for US-8.1's order history endpoint, keyed by
  * customerId. TTL-only freshness (see spec.md's "Out of Scope") - no explicit
  * invalidation when an order is created or its status changes, so a write can
  * take up to the configured TTL to show up in a cached list.
  */
trait OrderHistoryCache[F[_]] {
  def get(customerId: String): F[Option[List[OrderResponse]]]
  def set(customerId: String, value: List[OrderResponse]): F[Unit]
}

object OrderHistoryCache {

  /** No TTL/expiry - only used for route-level tests that don't exercise expiry
    * behavior themselves (that's `OrderHistoryCacheSuite`'s job, against a real
    * Redis).
    */
  def inMemory[F[_]: Sync]: F[OrderHistoryCache[F]] =
    Ref.of[F, Map[String, List[OrderResponse]]](Map.empty).map { ref =>
      new OrderHistoryCache[F] {
        def get(customerId: String): F[Option[List[OrderResponse]]] =
          ref.get.map(_.get(customerId))
        def set(customerId: String, value: List[OrderResponse]): F[Unit] =
          ref.update(_ + (customerId -> value))
      }
    }

  private def keyFor(customerId: String): String = s"order-history:$customerId"

  def fromRedisCommands[F[_]: Async](
      commands: RedisCommands[F, String, String],
      ttl: FiniteDuration
  ): OrderHistoryCache[F] =
    new OrderHistoryCache[F] {
      def get(customerId: String): F[Option[List[OrderResponse]]] =
        commands
          .get(keyFor(customerId))
          .map(_.flatMap(decode[List[OrderResponse]](_).toOption))

      def set(customerId: String, value: List[OrderResponse]): F[Unit] =
        commands.setEx(keyFor(customerId), value.asJson.noSpaces, ttl)
    }

  def resource[F[_]: Async](
      config: RedisConfig,
      logger: StructuredLogger[F]
  ): Resource[F, OrderHistoryCache[F]] = {
    given Log[F] = log4CatsInstance(using logger)
    given MkRedis[F] = MkRedis.forAsync[F]
    Redis[F]
      .utf8(config.uri)
      .map(
        fromRedisCommands(
          _,
          FiniteDuration(config.historyTtlSeconds, "seconds")
        )
      )
  }
}
