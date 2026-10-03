package orderservice

import cats.effect.IO
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import munit.CatsEffectSuite
import org.testcontainers.utility.DockerImageName

import java.sql.DriverManager

class MigrationsSuite extends CatsEffectSuite with TestContainerForAll {

  override val containerDef: PostgreSQLContainer.Def =
    PostgreSQLContainer.Def(dockerImageName =
      DockerImageName.parse("postgres:16-alpine")
    )

  test("running migrations creates the order table") {
    withContainers { postgres =>
      val config = PostgresConfig(
        host = postgres.host,
        port = postgres.mappedPort(5432),
        database = postgres.databaseName,
        user = postgres.username,
        password = postgres.password
      )

      Migrations.run[IO](config).map { _ =>
        val conn = DriverManager.getConnection(
          postgres.jdbcUrl,
          postgres.username,
          postgres.password
        )
        try {
          val rs = conn
            .createStatement()
            .executeQuery(
              "select column_name, data_type from information_schema.columns where table_name = 'order' order by ordinal_position"
            )
          val columns = Iterator
            .unfold(())(_ =>
              if (rs.next()) Some((rs.getString("column_name"), ())) else None
            )
            .toList
          assertEquals(
            columns,
            List(
              "id",
              "customer_id",
              "total_cents",
              "status",
              "created_at",
              "updated_at"
            )
          )
        } finally conn.close()
      }
    }
  }

  test(
    "running migrations adds a CHECK constraint that rejects an invalid status"
  ) {
    withContainers { postgres =>
      val config = PostgresConfig(
        host = postgres.host,
        port = postgres.mappedPort(5432),
        database = postgres.databaseName,
        user = postgres.username,
        password = postgres.password
      )

      Migrations.run[IO](config).map { _ =>
        val conn = DriverManager.getConnection(
          postgres.jdbcUrl,
          postgres.username,
          postgres.password
        )
        try {
          val stmt = conn.createStatement()
          intercept[java.sql.SQLException] {
            stmt.executeUpdate(
              "insert into \"order\" (id, customer_id, total_cents, status) " +
                "values (gen_random_uuid(), 'cust-1', 100, 'bogus')"
            )
          }
        } finally conn.close()
      }
    }
  }

  test(
    "running migrations' CHECK constraint still allows the three valid statuses"
  ) {
    withContainers { postgres =>
      val config = PostgresConfig(
        host = postgres.host,
        port = postgres.mappedPort(5432),
        database = postgres.databaseName,
        user = postgres.username,
        password = postgres.password
      )

      Migrations.run[IO](config).map { _ =>
        val conn = DriverManager.getConnection(
          postgres.jdbcUrl,
          postgres.username,
          postgres.password
        )
        try {
          val stmt = conn.createStatement()
          List("pending", "reserved", "reservation_failed").foreach { status =>
            stmt.executeUpdate(
              "insert into \"order\" (id, customer_id, total_cents, status) " +
                s"values (gen_random_uuid(), 'cust-1', 100, '$status')"
            )
          }
        } finally conn.close()
      }
    }
  }

  test("running migrations creates the order_items table") {
    withContainers { postgres =>
      val config = PostgresConfig(
        host = postgres.host,
        port = postgres.mappedPort(5432),
        database = postgres.databaseName,
        user = postgres.username,
        password = postgres.password
      )

      Migrations.run[IO](config).map { _ =>
        val conn = DriverManager.getConnection(
          postgres.jdbcUrl,
          postgres.username,
          postgres.password
        )
        try {
          val rs = conn
            .createStatement()
            .executeQuery(
              "select column_name, data_type from information_schema.columns where table_name = 'order_items' order by ordinal_position"
            )
          val columns = Iterator
            .unfold(())(_ =>
              if (rs.next()) Some((rs.getString("column_name"), ())) else None
            )
            .toList
          assertEquals(
            columns,
            List(
              "id",
              "order_id",
              "sku",
              "product_name",
              "unit_price_cents",
              "quantity"
            )
          )
        } finally conn.close()
      }
    }
  }

  test(
    "order_items.order_id has a FK constraint - an unrelated order_id is rejected"
  ) {
    withContainers { postgres =>
      val config = PostgresConfig(
        host = postgres.host,
        port = postgres.mappedPort(5432),
        database = postgres.databaseName,
        user = postgres.username,
        password = postgres.password
      )

      Migrations.run[IO](config).map { _ =>
        val conn = DriverManager.getConnection(
          postgres.jdbcUrl,
          postgres.username,
          postgres.password
        )
        try {
          val stmt = conn.createStatement()
          intercept[java.sql.SQLException] {
            stmt.executeUpdate(
              "insert into order_items (id, order_id, sku, product_name, unit_price_cents, quantity) " +
                "values (gen_random_uuid(), gen_random_uuid(), 'sku-1', 'Widget', 999, 2)"
            )
          }
        } finally conn.close()
      }
    }
  }

  test(
    "deleting an order cascades to delete its order_items rows"
  ) {
    withContainers { postgres =>
      val config = PostgresConfig(
        host = postgres.host,
        port = postgres.mappedPort(5432),
        database = postgres.databaseName,
        user = postgres.username,
        password = postgres.password
      )

      Migrations.run[IO](config).map { _ =>
        val conn = DriverManager.getConnection(
          postgres.jdbcUrl,
          postgres.username,
          postgres.password
        )
        try {
          val stmt = conn.createStatement()
          val orderId = java.util.UUID.randomUUID()
          val itemId = java.util.UUID.randomUUID()
          stmt.executeUpdate(
            s"insert into \"order\" (id, customer_id, total_cents, status) " +
              s"values ('$orderId', 'cust-1', 100, 'pending')"
          )
          stmt.executeUpdate(
            "insert into order_items (id, order_id, sku, product_name, unit_price_cents, quantity) " +
              s"values ('$itemId', '$orderId', 'sku-1', 'Widget', 999, 2)"
          )
          stmt.executeUpdate(s"delete from \"order\" where id = '$orderId'")
          val rs = stmt.executeQuery(
            s"select count(*) from order_items where id = '$itemId'"
          )
          rs.next()
          assertEquals(rs.getInt(1), 0)
        } finally conn.close()
      }
    }
  }
}
