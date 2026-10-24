package orderservice

import cats.effect.{Resource, Sync}
import cats.syntax.all._
import org.flywaydb.core.Flyway

object Migrations {

  private def jdbcUrl(postgres: PostgresConfig): String =
    s"jdbc:postgresql://${postgres.host}:${postgres.port}/${postgres.database}"

  def run[F[_]: Sync](postgres: PostgresConfig): F[Unit] =
    Sync[F].blocking {
      Flyway
        .configure()
        .dataSource(jdbcUrl(postgres), postgres.user, postgres.password)
        .load()
        .migrate()
    }.void

  def resource[F[_]: Sync](postgres: PostgresConfig): Resource[F, Unit] =
    Resource.eval(run[F](postgres))
}
