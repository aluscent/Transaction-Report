package com.alitariverdy

import db.PostgresConnector
import data.rows.Guarantee
import com.typesafe.config.ConfigFactory
import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import java.sql.DriverManager
import scala.util.Using

class ReportPublishSpec extends AnyFunSuite with ForAllTestContainer with BeforeAndAfterAll {
  override val container: PostgreSQLContainer = PostgreSQLContainer()
  implicit lazy val spark: SparkSession = SparkSession.builder().master("local[2]")
    .appName("report-publish").config("spark.ui.enabled", "false").getOrCreate()
  override def afterAll(): Unit = { spark.stop(); super.afterAll() }
  test("reruns replace only their date and a failed publish preserves the previous report") {
    import spark.implicits._
    val config = ConfigFactory.parseString(s"""
      url = "${container.jdbcUrl}"
      driver = "org.postgresql.Driver"
      checkpointLocation = "/tmp/report-test"
    """)
    val connector = new PostgresConnector(config, Map(
      "POSTGRES_USER" -> container.username, "POSTGRES_PASSWORD" -> container.password))
    def row(name: String, date: String, amount: Int) = Guarantee(name, "GB", date,
      BigDecimal(amount), BigDecimal(amount), BigDecimal(0), BigDecimal(0),
      BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))
    // Let JDBC create the same shape, then add a constraint to force a publish failure.
    connector.writeTable(Seq(row("keep", "2025-01-01", 1)).toDS(), """"public"."guarantees"""")
    connector.replaceReport(Seq(row("first", "2025-01-02", 2)).toDS(), """"public"."guarantees"""", "2025-01-02")
    connector.replaceReport(Seq(row("replacement", "2025-01-02", 3)).toDS(), """"public"."guarantees"""", "2025-01-02")
    Using.resource(DriverManager.getConnection(container.jdbcUrl, container.username, container.password)) { connection =>
      Using.resource(connection.createStatement()) { statement =>
        statement.execute("ALTER TABLE public.guarantees ADD CONSTRAINT positive_amount CHECK (class_a_sum >= 0)")
        intercept[Exception] {
          connector.replaceReport(Seq(row("bad", "2025-01-02", -1)).toDS(), """"public"."guarantees"""", "2025-01-02")
        }
        Using.resource(statement.executeQuery("SELECT guarantor_name FROM public.guarantees ORDER BY guarantor_name")) { result =>
          val names = scala.collection.mutable.ListBuffer.empty[String]
          while (result.next()) names += result.getString(1)
          assert(names.toList == List("keep", "replacement"))
        }
      }
    }
    connector.replaceReport(Seq.empty[Guarantee].toDS(), """"public"."guarantees"""", "2025-01-02")
    Using.resource(DriverManager.getConnection(container.jdbcUrl, container.username, container.password)) { connection =>
      Using.resource(connection.createStatement()) { statement =>
        Using.resource(statement.executeQuery("SELECT count(*) FROM public.guarantees WHERE partition_date = '2025-01-02'")) { result =>
          result.next(); assert(result.getInt(1) == 0)
        }
      }
    }
  }
}
