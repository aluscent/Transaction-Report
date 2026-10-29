package com.alitariverdy

import data.rows.Guarantee

import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{Assertion, BeforeAndAfterAll, BeforeAndAfterEach}
import org.testcontainers.containers.wait.strategy.Wait

import java.io.File
import scala.language.reflectiveCalls

class ReportSpec extends AnyFlatSpec with Report with BeforeAndAfterEach with BeforeAndAfterAll {

  var container: DockerComposeContainer = _

  override def beforeAll(): Unit = {


    sparkSession
  }

  override def beforeEach(): Unit = {
    // Start Docker Compose
    val dockerComposeFile = new File("src/test/resources/docker-compose.yml")
    container = DockerComposeContainer(
      dockerComposeFile,
      exposedServices = Seq(
        ExposedService("postgres", 5432, Wait.forListeningPort())
      )
    )

    container.start()
  }

  override def afterEach(): Unit = {
    container.stop()
  }

  override protected def afterAll(): Unit = {
    sparkSession.stop()
  }

  "Spark API guarantees report" should
    "be generated from Postgres data" in
    testReport(SparkApiReport)

  "Spark SQL guarantees report" should
    "be generated from Postgres data" in
    testReport(SparkSqlReport)

  def testReport(report: Report { def main(args: Array[String]): Unit }): Assertion = {
    import sparkSession.implicits._

    // Execute the report
    report.main(Array("2025-02-25"))

    // Schema to StructType conversion
    val schema = ScalaReflection
      .schemaFor[Guarantee]
      .dataType
      .asInstanceOf[StructType]

    val result = postgresConnector
      .readTable(s""""public"."guarantees"""", schema)
      .as[Guarantee]
      .collect()

    println(result.mkString("\n"))
    assert(result.length == 6)
    assert(result.exists(g => g.guarantor_name == "G_001" && g.class_a_sum == 600.0))
    assert(result.exists(g => g.guarantor_name == "G_002" && g.class_b_sum == 1000.0))
    assert(result.exists(g => g.guarantor_name == "G_004" && g.class_b_avg == 4000.0))
    assert(result.exists(g => g.guarantor_name == "G_005" && g.class_d_avg == 2700.0))
    assert(result.exists(g => g.guarantor_name == "G_006" && g.class_a_sum == 1260.0))
    assert(result.exists(g => g.guarantor_name == "G_007" && g.class_a_avg == 1260.0))
  }
}
