addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.4.4")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")
// Packages order-service as a Docker image (JavaAppPackaging + DockerPlugin).
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.11.1")
// purekafka's AsyncAPI-docs sbt plugin (ADR 0010 follow-up): generates
// target/asyncapi-docs-spec/asyncapi.json + target/asyncapi-docs/index.html
// from AsyncApiChannels automatically on every `sbt compile`, replacing the
// old generate-kafka-asyncapi-docs.sh script. Real GitHub Packages release -
// the meta-build needs its own resolver/credentials (build.sbt's don't
// apply here; this is project/, not the main build).
resolvers += "GitHub Packages" at "https://maven.pkg.github.com/beckfordp/purerest"

credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  sys.env.getOrElse("GITHUB_ACTOR", ""),
  sys.env.getOrElse("GITHUB_TOKEN", "")
)

// 0.3.0-dev auto-wires asyncApiGenerate's HTML into Universal/mappings for
// native-packager images (TD-6); 0.2.0-dev added schemaRegistryCheck
// (Rework 5 step 3, ADR 0003). Real GitHub Packages release.
addSbtPlugin("io.github.beckfordp" % "sbt-purekafka-docs" % "0.3.0-dev")
