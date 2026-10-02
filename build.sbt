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
// Pinned to match the purerestlib version this service is built against — see
// README's "Consuming purerest as a dependency" section.
val purerestlibVersion = "0.1.0"

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
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(
    name := "order-service",
    scalaVersion := scala3Version,
    version := "0.1.0",
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
      "com.dimafeng" %% "testcontainers-scala-munit" % testcontainersScalaVersion % Test,
      // log4cats-testing: purerestlib keeps this Test-scoped (doesn't propagate to
      // consumers), so this service declares its own copy to assert on log output
      // (StructuredTestingLogger) in its own tests.
      "org.typelevel" %% "log4cats-testing" % log4catsVersion % Test
    )
  )
