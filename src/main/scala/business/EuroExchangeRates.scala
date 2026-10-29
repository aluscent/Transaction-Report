package com.alitariverdy
package business

import data.PostgresExchangeRateRepository
import data.rows.{ExchangeRate, Transaction}
import db.PostgresConnector
import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.{Dataset, SparkSession}

object EuroExchangeRates {
  /** Latest complete EUR batch, with exactly one positive rate per currency.
    * Validation is eager so missing reference data cannot disappear in an inner join.
    */
  def validate(transactions: Dataset[Transaction], rates: Dataset[ExchangeRate]): Unit = {
    require(rates.filter(r => r.from_currency == null || r.to_currency != "EUR" ||
      r.rate == null || r.rate <= 0).take(1).isEmpty, "Invalid EUR exchange rate")
    require(rates.groupBy("from_currency").count().filter(col("count") =!= 1).take(1).isEmpty,
      "Duplicate exchange rates in the selected batch")
    require(transactions.join(rates, transactions.col("currency") === rates.col("from_currency"),
      "left_anti").take(1).isEmpty, "Missing EUR exchange rate for a transaction currency")
  }

  def convert(transactions: Dataset[Transaction], rates: Dataset[ExchangeRate])(
      implicit ss: SparkSession
  ): Dataset[Transaction] = {
    import ss.implicits._
    validate(transactions, rates)
    transactions.joinWith(broadcast(rates), transactions.col("currency") === rates.col("from_currency"))
      .map { case (transaction, rate) =>
        transaction.copy(amount = transaction.amount * rate.rate, currency = "EUR")
      }
  }

  def apply(df: Dataset[Transaction], postgresConnector: PostgresConnector, reportDate: String)(
      implicit ss: SparkSession
  ): Dataset[Transaction] =
    convert(df, PostgresExchangeRateRepository(postgresConnector, reportDate).filteredTable)
}
