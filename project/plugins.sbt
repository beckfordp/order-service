addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.4.4")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")
// Packages order-service as a Docker image (JavaAppPackaging + DockerPlugin).
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.11.1")
// purekafka's AsyncAPI-docs sbt plugin (ADR 0010 follow-up): generates
// target/asyncapi-docs-spec/asyncapi.json + target/asyncapi-docs/index.html
// from AsyncApiChannels automatically on every `sbt compile`, replacing the
// old generate-kafka-asyncapi-docs.sh script. Resolves from ~/.ivy2/local
// (same as purekafka itself, below) - no release tag yet, a fixed dev
// version from the sibling purerest checkout's own `sbt
// sbtPureKafkaDocs/publishLocal`; re-pin once a real tag exists.
addSbtPlugin("io.github.beckfordp" % "sbt-purekafka-docs" % "0.1.0-dev")
