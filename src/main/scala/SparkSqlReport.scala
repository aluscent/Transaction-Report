package com.alitariverdy

import data.rows.Guarantee
import data.{PostgresExchangeRateRepository, PostgresReceiversRegistryRepository, PostgresTransactionRepository}
import db.SQLs

import com.alitariverdy.utils.GetDate
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.broadcast

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object SparkSqlReport extends Report {
  def main(args: Array[String]): Unit = {
    val reportDate: String = GetDate(args)

    import sparkSession.implicits._
    val transactions = PostgresTransactionRepository(postgresConnector, reportDate).filteredTable
    val valid = business.CheckRegisteredReceivers(transactions, postgresConnector, reportDate)
    val euros = business.EuroExchangeRates(valid, postgresConnector, reportDate)
    business.ParseGuarantors(euros).createOrReplaceTempView("allocated_transactions")

    println(SQLs.sqlReport(reportDate))

    val guarantees: Dataset[Guarantee] =
      sparkSession.sql(SQLs.sqlReport(reportDate)).as[Guarantee]

    postgresConnector.replaceReport(guarantees, """"public"."guarantees"""", reportDate)
  }
}
