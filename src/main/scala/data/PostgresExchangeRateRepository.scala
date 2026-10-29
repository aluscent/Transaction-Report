package com.alitariverdy
package data

import db.PostgresConnector

import com.alitariverdy.data.rows.ExchangeRate
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{col, lit, max}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

import java.sql.Date

class PostgresExchangeRateRepository(val postgresConnector: PostgresConnector, reportDate: String)(implicit
    ss: SparkSession,
    eer: Encoder[ExchangeRate],
    es: Encoder[String]
) extends PostgresRepository[ExchangeRate]("exchange_rates") {

  // The date of the last batch of exchange rates
  private val latestExchangeRatesDate: Broadcast[String] = ss.sparkContext.broadcast(
    table
      .filter(col("partition_date") <= lit(reportDate))
      .select(max(col("partition_date")))
      .as[String]
      .collect()
      .head
  )

  override lazy val filteredTable: Dataset[ExchangeRate] = table
    .filter(t => t.to_currency == "EUR" && t.partition_date == latestExchangeRatesDate.value)
}

object PostgresExchangeRateRepository {
  def apply(
      postgresConnector: PostgresConnector, reportDate: String
  )(implicit ss: SparkSession): PostgresExchangeRateRepository = {
    import ss.implicits._
    new PostgresExchangeRateRepository(postgresConnector, reportDate)
  }
}
