# order-service

A production-quality microservice built on
[purerest](https://github.com/beckfordp/purerest), generated from the
[pure-service-generator](https://github.com/beckfordp/pure-service-generator) giter8 template.
Its own `build.sbt` resolves `purerestlib` as a published GitHub Packages dependency.

## Prerequisites

- sbt / JDK 21 (for building and running)
- Docker Desktop (or another Docker engine) with Docker Compose v2
- A GitHub [personal access token](https://github.com/settings/tokens) with `read:packages`
  scope, exported as `GITHUB_TOKEN` (and `GITHUB_ACTOR` set to your GitHub username) — needed to
  resolve `purerestlib` from GitHub Packages. GitHub Packages requires authentication to *read*
  Maven artifacts even from a public repo.

## Quickstart

```
export GITHUB_ACTOR=<your-github-username>
export GITHUB_TOKEN=<your-PAT-with-read:packages>

docker compose up -d   # starts Postgres
sbt run                # runs migrations, then starts the service on :8080
```

Then, in another terminal:

```
# Create a order
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" -d '{"item":"widget","quantity":3}'

# Read it back (substitute the id from the response above)
curl http://localhost:8080/orders/<id>

# Partially update it (quantity/status)
curl -X PATCH http://localhost:8080/orders/<id> \
  -H "Content-Type: application/json" -d '{"quantity":5,"status":"shipped"}'

# Or fully replace it (same required fields as PATCH — this resource has no
# other client-writable ones — but PUT is idempotent full-replace semantics)
curl -X PUT http://localhost:8080/orders/<id> \
  -H "Content-Type: application/json" -d '{"quantity":5,"status":"shipped"}'

# Delete it
curl -X DELETE http://localhost:8080/orders/<id>

# Liveness / readiness
curl http://localhost:8080/health
curl http://localhost:8080/health/ready
```

Swagger UI (generated from the same tapir endpoint definitions as the real routes — see
`purerest.docs.Docs`) is browsable at **http://localhost:8080/docs**.

## Development guidelines

Extending this service? See [`development-guidelines.md`](./development-guidelines.md) for the
pure-FP/tagless-final standards it follows — error modeling as ADTs, patterns to follow (and
avoid) — before adding new code.

## Testing

```
sbt scalafmtCheck test
```

Unit tests use an in-memory `OrderStore`; Postgres-backed tests spin up a
real, ephemeral container via Testcontainers — no local Postgres or manual setup needed to run
`sbt test`.

## One database per service

This service owns a single dedicated Postgres database, named after its domain: a database named
`order` (singular), with a `order` table (quoted throughout in SQL — several
plausible domain names, e.g. "user"/"group"/"order", are reserved PostgreSQL keywords). REST
paths stay pluralized (`/orders`, `/orders/{id}`) per ordinary
resource-collection convention; only the database/table naming reflects the one-db-per-service
rule. No service reads or writes another service's database directly.

## Calling other services with resilience

This service doesn't call any other service out of the box, so purerest's retry + circuit-breaker
middleware (`purerest.resilience.Resilience.middleware`) isn't wired into `Main`. When you adapt
it to call a real downstream service, wrap its http4s `Client[F]` with `Resilience.middleware`
before building your own client on top of it — see
[`ClientResilienceExampleSuite`](./src/test/scala/orderservice/examples/ClientResilienceExampleSuite.scala)
for the pattern, tested against a dummy client so it stays correct as purerest evolves.
