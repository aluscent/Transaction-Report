package com.alitariverdy
package db

import data.rows.Guarantee

import com.typesafe.config.Config
import java.sql.DriverManager
import java.time.LocalDate
import java.util.UUID
import scala.util.Using
import org.apache.spark.sql.functions.col
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

class PostgresConnector(postgresConfig: Config, credentials: Map[String, String] = sys.env)(implicit sparkSession: SparkSession)
    extends Serializable {

  private val connectionProperties: Broadcast[Map[String, String]] =
    sparkSession.sparkContext.broadcast(
      Map(
        "user" -> credentials("POSTGRES_USER"),
        "password" -> credentials("POSTGRES_PASSWORD"),
        "driver" -> postgresConfig.getString("driver"),
        "url" -> postgresConfig.getString("url"),
        "checkpointLocation" -> postgresConfig.getString("checkpointLocation")
      )
    )

  def readTable(tableFQN: String, schema: StructType): DataFrame = sparkSession.read
    .format("jdbc")
//    .schema(schema)
    .option("dbtable", tableFQN)
    .option("pushDownPredicate", "true")
    .options(connectionProperties.value)
    .load()

  /** Publish a complete report date atomically. Spark writes only to staging;
    * failed batches leave the existing report untouched, and reruns replace it.
    */
  def replaceReport(df: Dataset[Guarantee], tableFQN: String, reportDate: String): Unit = {
    LocalDate.parse(reportDate)
    val Target = """"([A-Za-z_][A-Za-z0-9_]*)"\."([A-Za-z_][A-Za-z0-9_]*)"""".r
    val (schema, table) = tableFQN match {
      case Target(schema, table) => (schema, table)
      case _ => throw new IllegalArgumentException("Use a quoted schema.table target")
    }
    require(df.filter(col("partition_date").isNull || col("partition_date") =!= reportDate).take(1).isEmpty,
      "Every output row must belong to the requested report date")
    val stage = s"\"$schema\".\"report_stage_${UUID.randomUUID().toString.replace("-", "")}\""
    val columns = df.columns.map { name =>
      require(name.matches("[A-Za-z_][A-Za-z0-9_]*"), "Invalid report column")
      s"\"$name\""
    }.mkString(", ")
    val options = connectionProperties.value
    Class.forName(options("driver"))
    Using.resource(DriverManager.getConnection(options("url"), options("user"), options("password"))) { connection =>
      def execute(sql: String): Unit = Using.resource(connection.createStatement())(_.execute(sql))
      execute(s"CREATE TABLE $stage (LIKE $tableFQN INCLUDING DEFAULTS)")
      try {
        execute(s"ALTER TABLE $stage ADD CHECK (partition_date = '$reportDate' AND partition_date IS NOT NULL)")
        execute(s"ALTER TABLE $stage ADD UNIQUE (guarantor_name, country, partition_date)")
        df.write.format("jdbc").option("dbtable", stage).options(options).mode(SaveMode.Append).save()
        connection.setAutoCommit(false)
        try {
          execute(s"LOCK TABLE $tableFQN IN SHARE ROW EXCLUSIVE MODE")
          Using.resource(connection.prepareStatement(s"DELETE FROM $tableFQN WHERE partition_date = ?")) { statement =>
            statement.setString(1, reportDate)
            statement.executeUpdate()
          }
          execute(s"INSERT INTO $tableFQN ($columns) SELECT $columns FROM $stage")
          connection.commit()
        } catch {
          case error: Throwable => connection.rollback(); throw error
        } finally connection.setAutoCommit(true)
      } finally execute(s"DROP TABLE IF EXISTS $stage")
    }
  }

  // Low-level append retained for callers that explicitly want append semantics.
  def writeTable(df: Dataset[Guarantee], tableFQN: String): Unit = df.write
    .format("jdbc")
    .option("dbtable", tableFQN)
    .options(connectionProperties.value)
    .mode(SaveMode.Append)
    .save()
}

object PostgresConnector {

  /** Wraps methods related to Postgres for ease of use.
    * @param postgresConfig config loaded from "app.postgres"
    * @param sparkSession the Spark session used by main app
    * @return an object of type [[PostgresConnector]]
    */
  def apply(postgresConfig: Config)(implicit sparkSession: SparkSession) =
    new PostgresConnector(postgresConfig)
}
