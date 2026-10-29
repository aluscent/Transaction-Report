package com.alitariverdy

import business.{CheckRegisteredReceivers, EuroExchangeRates, ParseGuarantors, PivotTransactionType}
import data.PostgresTransactionRepository

import com.alitariverdy.utils.GetDate

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Implements Report using Spark's Dataframes API
 */
object SparkApiReport extends Report {
  def main(args: Array[String]): Unit = {
    val reportDate: String = GetDate(args)

    val transactions = PostgresTransactionRepository(postgresConnector, reportDate).filteredTable

    val receiversFiltered = CheckRegisteredReceivers(transactions, postgresConnector, reportDate)

    val convertedToEuro = EuroExchangeRates(receiversFiltered, postgresConnector, reportDate)

    import sparkSession.implicits._
    val guarantorsAdded = ParseGuarantors(convertedToEuro)

    val guarantees = PivotTransactionType(reportDate, guarantorsAdded)

    postgresConnector.replaceReport(guarantees, """"public"."guarantees"""", reportDate)
  }
}
