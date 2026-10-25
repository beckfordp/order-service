val scala3Version = "3.9.0"

val catsEffectVersion = "3.7.0"
val http4sVersion = "0.23.37"
val circeVersion = "0.14.16"
val munitVersion = "1.3.6"
val munitCatsEffectVersion = "2.2.1"
val log4catsVersion = "2.8.0"
val tapirVersion = "1.11.25"
val skunkVersion = "1.0.0"
val flywayVersion = "11.8.2"
val postgresqlJdbcVersion = "42.7.13"
val pureconfigVersion = "0.17.10"
val testcontainersScalaVersion = "0.43.6"
val fs2KafkaVersion = "3.6.0"
val redis4catsVersion = "2.0.6"
// Pinned to match the purerestlib version this service is built against — see
// README's "Consuming purerest as a dependency" section.
val purerestlibVersion = "0.1.0"
// Real GitHub Packages release (v0.3.0 - adds purekafka.docs.SchemaRegistryWriter,
// Rework 5 step 3/ADR 0003) — resolved via the same resolver/credentials as
// purerestlib below.
val purekafkaVersion = "0.3.0"

// Skunk 1.0.0 depends on otel4s-core/-core-common/-core-metrics 0.16.0 (its own
// optional tracing integration), which conflicts with purerestlib's otel4s 1.1.0
// (an early-semver 0.x -> 1.x jump, which sbt's version-scheme conflict detection
// treats as an error by default). "Highest version wins" is the correct
// resolution here — nothing in this project invokes Skunk's otel4s integration —
// so these three coordinates are pinned explicitly rather than left to eviction.
// Mirrors purerest's own build.sbt (see its "Transitive version drift" note).
ThisBuild / dependencyOverrides ++= Seq(
  "org.typelevel" %% "otel4s-core" % "1.1.0",
  "org.typelevel" %% "otel4s-core-common" % "1.1.0",
  "org.typelevel" %% "otel4s-core-metrics" % "1.1.0"
)

lazy val root = project
  .in(file("."))
  .enablePlugins(JavaAppPackaging, DockerPlugin, PureKafkaDocsPlugin)
  .settings(
    name := "order-service",
    scalaVersion := scala3Version,
    version := "0.1.0",
    // Drives the sbt-purekafka-docs plugin's asyncApiGenerateSpec/
    // asyncApiGenerate tasks, auto-chained after Compile/compile - see
    // AsyncApiChannels.scala (orderservice.AsyncApiChannels, an
    // AsyncApiChannelsProvider). Title/version match Main.scala's own
    // AsyncApiDocs.routes[IO]("Order Service", "1.0", ...) call for the
    // live /docs/asyncapi.json route - otherwise the build-time generated
    // docs default to name.value/version.value ("order-service"/"0.1.0")
    // and silently disagree with what's actually served live.
    asyncApiChannelsClass := "orderservice.AsyncApiChannels",
    asyncApiTitle := "Order Service",
    asyncApiVersion := "1.0",
    // Drives schemaRegistryCheck (Rework 5 step 3, ADR 0003) - reuses the
    // same AsyncApiChannels registrations above. Env-var override (not a
    // hardcoded default) because local dev and CI point at two different,
    // unrelated Apicurio instances: locally, the shared OrbStack cluster
    // (default here); in CI, a throwaway `docker run` container with no
    // OrbStack/cluster-DNS access (see ci.yml).
    schemaRegistryUrl := sys.env.getOrElse(
      "SCHEMA_REGISTRY_URL",
      "http://apicurio-registry.gluon-local.svc.cluster.local:8080"
    ),
    // One shared group across every service (not the plugin's own
    // per-service default, name.value) - confirmed live 2026-10-25 that
    // "one group per service" meant a producer's and a consumer's
    // registration of the *same* real topic/eventType contract (e.g.
    // inventory-service producing inventory-events-StockReserved,
    // order-service consuming it) landed in two totally independent
    // groups/version-lineages, so the BACKWARD compatibility check never
    // actually compared them against each other - exactly the
    // producer/consumer mismatch ADR 0003 exists to catch. A shared
    // group means artifactId (<topic>-<eventType>, already globally
    // unique) is the only key that matters, so whichever side registers
    // second is checked against whichever registered first, regardless
    // of which service that is.
    schemaRegistryGroupId := "gluon",
    // Bakes asyncApiGenerate's output into the Docker image itself -
    // previously only ever worked via a bare `sbt run`; neither
    // docker-compose nor the k8s deployment ever packaged these files in
    // (confirmed live: 404 in both). Mapped under the exact same relative
    // path (`target/asyncapi-docs`) AppRoutes.scala's
    // staticAsyncApiDocsDir already looks for, so no code change needed
    // there - the image's working directory (/opt/docker, confirmed via
    // `kubectl exec ... pwd`) just needs that directory to actually
    // exist. `asyncApiGenerate.value` (not just the output-dir setting)
    // forces generation to have happened before mapping; sbt caches the
    // task's result within one command invocation, so this doesn't
    // re-run the Node-based generator a second time - Compile/compile's
    // own existing taskDyn chain already triggered it moments earlier in
    // the same build.
    Universal / mappings ++= {
      val base = asyncApiGenerate.value
      Path
        .allSubpaths(base)
        .map { case (file, relPath) =>
          file -> s"target/asyncapi-docs/$relPath"
        }
        .toSeq
    },
    // purerestlib resolved as an ordinary published artifact from GitHub
    // Packages, not a source/`.dependsOn` link back to the purerest repo —
    // the same real-external-consumer proof purerest's own smoke-test/
    // module established. Requires GITHUB_ACTOR/GITHUB_TOKEN (a PAT with
    // read:packages scope) in the environment.
    resolvers += "GitHub Packages" at "https://maven.pkg.github.com/beckfordp/purerest",
    credentials += Credentials(
      "GitHub Package Registry",
      "maven.pkg.github.com",
      sys.env.getOrElse("GITHUB_ACTOR", ""),
      sys.env.getOrElse("GITHUB_TOKEN", "")
    ),
    // Docker image for local dev via `docker compose up` — see docker-compose.yml.
    Docker / packageName := "order-service",
    dockerBaseImage := "eclipse-temurin:21-jre",
    dockerUpdateLatest := true,
    dockerExposedPorts := Seq(8080, 9090),
    Universal / javaOptions += "-Dlogback.configurationFile=logback-docker.xml",
    libraryDependencies ++= Seq(
      "io.github.beckfordp" %% "purerestlib" % purerestlibVersion,
      // purekafka: Kafka resilience/observability combinators (stream
      // supervision, dead-letter routing) for StockEventConsumer/
      // PaymentEventConsumer — see TD-2.2/TD-2.4, ADR 0010.
      "io.github.beckfordp" %% "purekafka" % purekafkaVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "org.http4s" %% "http4s-ember-server" % http4sVersion,
      // http4s-ember-client: outbound HTTP calls to other services (e.g. the
      // synchronous stock-reservation call to inventory-service), wrapped in
      // purerest's Resilience.middleware - see README's "Calling other
      // services with resilience".
      "org.http4s" %% "http4s-ember-client" % http4sVersion,
      "org.http4s" %% "http4s-dsl" % http4sVersion,
      "org.http4s" %% "http4s-circe" % http4sVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-core" % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-json-circe" % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-http4s-server" % tapirVersion,
      // skunk: non-blocking, pure-FP Postgres access — this service's persistence layer.
      "org.tpolecat" %% "skunk-core" % skunkVersion,
      // fs2-kafka: pure-FP, FS2-native Kafka consumer (US-5.2) - subscribes to
      // inventory.stock-reserved/-reservation-failed to update order status.
      "com.github.fd4s" %% "fs2-kafka" % fs2KafkaVersion,
      // redis4cats: cats-effect-native Redis client (US-8.1) - caches a
      // customer's order history. -log4cats wires its internal logging
      // through the same log4cats this service already uses.
      "dev.profunktor" %% "redis4cats-effects" % redis4catsVersion,
      "dev.profunktor" %% "redis4cats-log4cats" % redis4catsVersion,
      // flyway: JDBC-based schema migration tool, run on startup to create/update the
      // order table. Independent of Skunk (which handles all runtime queries).
      "org.flywaydb" % "flyway-core" % flywayVersion,
      "org.flywaydb" % "flyway-database-postgresql" % flywayVersion,
      // postgresql (pgjdbc): build-only JDBC driver, used solely by Flyway to run
      // migrations. Runtime-only: never referenced directly in code, loaded by
      // Flyway/JDBC's DriverManager via SPI.
      "org.postgresql" % "postgresql" % postgresqlJdbcVersion % Runtime,
      // pureconfig: loads application.conf into typed config case classes.
      "com.github.pureconfig" %% "pureconfig-core" % pureconfigVersion,
      // munit: test framework used across this project (Scala-native, no JUnit dependency).
      "org.scalameta" %% "munit" % munitVersion % Test,
      // munit-cats-effect: lets test bodies return IO[Unit] and run under munit directly.
      "org.typelevel" %% "munit-cats-effect" % munitCatsEffectVersion % Test,
      // testcontainers-scala: spins up a real, ephemeral Postgres container for
      // integration tests (not used by main code).
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersScalaVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-kafka" % testcontainersScalaVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-redis" % testcontainersScalaVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-munit" % testcontainersScalaVersion % Test,
      // log4cats-testing: purerestlib keeps this Test-scoped (doesn't propagate to
      // consumers), so this service declares its own copy to assert on log output
      // (StructuredTestingLogger) in its own tests.
      "org.typelevel" %% "log4cats-testing" % log4catsVersion % Test
    )
  )
