# Redis as a pass-through cache (US-8.1)

How order-service caches a customer's order history in Redis, and how it
uses `redis4cats` (the cats-effect-native Redis client) to do it. This is
the first Redis integration anywhere in Gluon — no prior pattern existed to
copy, so this doc exists for the next service that needs one (cart-service's
own Redis rework is still unimplemented at the time of writing).

## The pattern: cache-aside, TTL-only freshness

`GET /orders?customerId=<id>` (order history) is read-mostly and tolerant of
brief staleness, so it uses the simplest possible caching strategy:
**cache-aside** (a.k.a. "pass-through" or "lazy-loading") with a plain TTL —
no write-through, no explicit invalidation.

```
GET /orders?customerId=X
        │
        ▼
  cache.get(X) ──── hit ────► return cached list
        │
       miss
        ▼
  store.listByCustomer(X)
        │
        ▼
  cache.set(X, result, ttl)
        │
        ▼
  return result
```

Concretely, in `OrderRoutes.listOrdersServerEndpoint`:

```scala
cached <- historyCache.get(customerId)
result <- cached match {
  case Some(history) => IO.pure(history)          // hit: done
  case None =>                                     // miss:
    for {
      orders  <- store.listByCustomer(customerId)  //   1. query the store
      history  = orders.map(OrderResponse(_, _))
      _       <- historyCache.set(customerId, history) // 2. populate the cache
    } yield history                                //   3. return it
}
```

**What "TTL-only freshness" means in practice:** when an order is created or
its status changes (checkout, or the async Kafka consumer), *nothing*
proactively updates or evicts that customer's cached entry. The cache simply
expires on its own after `history-ttl-seconds` (default 60s, see
`application.conf`), at which point the next request re-populates it from
the store. A write can therefore take up to the TTL to show up in a cached
read. That tradeoff is deliberate — see the `order-history-cache_20261002` track's
`spec.md` (in `conductor/tracks/` or `conductor/archive/`, depending on
whether it's been archived) "Out of Scope" section for the reasoning — and
is the right default for most read-mostly caches. If a future cache needs read-your-writes consistency,
reach for explicit invalidation (evict/update the key at the same write
site) instead of a shorter TTL.

## The redis4cats API

[`redis4cats`](https://github.com/profunktor/redis4cats) is Profunktor's
cats-effect/fs2-native Redis client, built on Lettuce. order-service depends
on two of its modules (`build.sbt`):

```scala
"dev.profunktor" %% "redis4cats-effects"  % "2.0.6"
"dev.profunktor" %% "redis4cats-log4cats" % "2.0.6"
```

### Connecting

A connection is a `Resource` — like Skunk's `Session` or a Doobie
transactor, it's acquired once and the underlying Lettuce client is released
automatically when the resource is finalized:

```scala
import dev.profunktor.redis4cats.Redis
import dev.profunktor.redis4cats.RedisCommands

val connection: Resource[F, RedisCommands[F, String, String]] =
  Redis[F].utf8("redis://localhost:6379")
```

`RedisCommands[F, K, V]` is the full command surface (strings, hashes,
lists, sets, pub/sub, transactions, ...) for a given key/value codec —
`.utf8` fixes both `K` and `V` to `String`, which is all this cache needs
(JSON-encoded blobs under string keys).

### The two implicits you have to provide yourself

`Redis[F].utf8(...)` needs two typeclass instances in scope that are **not**
automatically derived for an arbitrary `F[_]: Async` — you have to construct
them explicitly:

```scala
import dev.profunktor.redis4cats.effect.{Log, MkRedis}
import dev.profunktor.redis4cats.log4cats.log4CatsInstance

given Log[F] = log4CatsInstance(using logger)   // from a StructuredLogger[F] you already have
given MkRedis[F] = MkRedis.forAsync[F]          // from your Async[F] instance
```

- **`Log[F]`** is redis4cats' own minimal logging typeclass (it doesn't
  depend on log4cats directly, to stay log-library-agnostic). The
  `redis4cats-log4cats` module bridges it from any `org.typelevel.log4cats.Logger[F]`
  — order-service's `StructuredLogger[F]` qualifies directly.
- **`MkRedis[F]`** is what actually knows how to construct a Lettuce client
  for your effect type. `MkRedis.forAsync[F]` builds one from `Async[F]` +
  `Log[F]`.

Skipping either of these just fails to compile (`Redis[F].utf8` won't find
an implicit) — there's no silent fallback to get wrong.

### Reading and writing

The two methods this cache actually uses, from `redis4cats`' `Setter`/`Getter`
algebras:

```scala
def get(key: K): F[Option[V]]
def setEx(key: K, value: V, ttl: FiniteDuration): F[Unit]
```

`get` returns `None` for a missing key — that's the cache-miss signal, no
exception involved. `setEx` is `SET key value EX <seconds>` in one round
trip: the TTL is baked into the write itself, so there's no separate
`EXPIRE` call to forget.

### Where it all comes together

`OrderHistoryCache.scala` wraps `RedisCommands[F, String, String]` behind a
small domain-shaped interface (`get(customerId)`/`set(customerId, orders)`)
so the rest of the codebase never touches `redis4cats` types directly:

```scala
trait OrderHistoryCache[F[_]] {
  def get(customerId: String): F[Option[List[OrderResponse]]]
  def set(customerId: String, value: List[OrderResponse]): F[Unit]
}
```

- **Key shape:** `order-history:<customerId>` — one string key per
  customer, holding their whole history as a single value (see "Why one key
  per customer, not per order" below).
- **Serialization:** the value is `value.asJson.noSpaces` (circe) on write,
  `decode[List[OrderResponse]](_).toOption` on read — `OrderResponse` already
  has a `Codec` from the REST layer, reused here rather than inventing a
  second representation.
- **Construction:** `OrderHistoryCache.resource[F](config: RedisConfig, logger)`
  does the `given` setup above and returns the connected cache as a
  `Resource[F, OrderHistoryCache[F]]`, consistent with how `OrderStore.postgres`
  hands back a `Resource` too. `Main.scala` opens it once at startup,
  nested alongside the Postgres pool and HTTP client.
- **Testing:** `OrderHistoryCache.inMemory[F]` (a `Ref`-backed stub, no TTL)
  stands in wherever a test needs *a* cache but isn't testing caching itself
  — e.g. most of `OrderRoutesSuite`. The real Redis-backed implementation is
  exercised directly by `OrderHistoryCacheSuite`, against a real ephemeral
  Redis via `testcontainers-scala-redis` (miss, hit, and genuine TTL expiry
  — not simulated).

### Why one key per customer, not per order

The cache is keyed by `customerId`, holding that customer's *entire* history
as one JSON array — not one key per order. That matches the access pattern
exactly: the only read is "give me everything for this customer," so there's
nothing to gain from finer-grained keys, and it keeps invalidation (if this
ever needs to graduate off TTL-only) a single key per write instead of a
fan-out across every order a customer has ever placed.

## Local dev

`docker-compose.yml` runs a plain `redis:7-alpine` container (no
persistence, no auth — matches the Postgres/Kafka services' "just enough to
run `sbt run` locally" scope). `application.conf`'s `redis` block points at
it by default; override with `REDIS_URI` / `ORDER_HISTORY_CACHE_TTL_SECONDS`
env vars if needed. `scripts/verify-order-history-cache.sh` is a working,
runnable example of driving the whole flow end-to-end and inspecting the
cache directly via `redis-cli`.
