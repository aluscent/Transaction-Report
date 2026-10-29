package com.alitariverdy

import data.rows.{ExchangeRate, ReceiversRegistry, Transaction}
import data.{PostgresExchangeRateRepository, PostgresReceiversRegistryRepository, PostgresTransactionRepository}
import db.PostgresConnector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Date

class DataSpec extends AnyFlatSpec with Report with BeforeAndAfterAll {

  val date: String = "2025-02-25"

  override def beforeAll(): Unit = {


    sparkSession
  }

  "Transactions table" should "be filtered on date and 'OPEN' status" in {
    import sparkSession.implicits._

    // Mock transactions
    val transactions = Seq(
      Transaction("TX1", """[{"NAME": "G_001", "PERCENTAGE": 0.5}]""",
        "Sender1", "Receiver1", "CLASS_A", "US", "USD", BigDecimal(100),
        "OPEN", Date.valueOf("2025-02-15"), "2025-02-20"
      ),
      Transaction("TX2", """[{"NAME": "G_002", "PERCENTAGE": 0.3}]""",
        "Sender2", "Receiver2", "CLASS_B", "ES", "JPY", BigDecimal(200),
        "CLOSED", Date.valueOf("2025-02-15"), "2025-02-20"
      ),
      Transaction("TX3", """[{"NAME": "G_002", "PERCENTAGE": 0.3}]""",
        "Sender3", "Receiver3", "CLASS_D", "ES", "JPY", BigDecimal(100),
        "OPEN", Date.valueOf("2024-12-20"), "2025-02-20"
      )
    ).toDF()

    // Mock connector
    val mockConnector = new PostgresConnector(config.getConfig("app.postgres")) {
      override def readTable(tableFQN: String, schema: StructType): DataFrame = transactions
    }

    val mockTransactionRepository = PostgresTransactionRepository(mockConnector, date)

    val mockFilteredData = mockTransactionRepository.filteredTable.collect()

    assert(mockFilteredData.length == 1)
    assert(mockFilteredData.map(_.transaction_id) sameElements Array("TX1"))
  }

  "ExchangeRate table" should "be filtered on latest date and 'EUR' as destination currency" in {
    import sparkSession.implicits._

    // Mock transactions
    val exchangeRates = Seq(
      ExchangeRate(
        "USD",
        "JPY",
        BigDecimal(0.008),
        "2025-01-25"
      ),
      ExchangeRate(
        "USD",
        "EUR",
        BigDecimal(0.9),
        "2025-01-25"
      ),
      ExchangeRate(
        "CAD",
        "EUR",
        BigDecimal(0.7),
        "2024-12-25"
      )
    ).toDF()

    // Mock connector
    val mockConnector = new PostgresConnector(config.getConfig("app.postgres")) {
      override def readTable(tableFQN: String, schema: StructType): DataFrame = exchangeRates
    }

    val mockExchangeRateRepository = PostgresExchangeRateRepository(mockConnector, date)

    val mockFilteredData = mockExchangeRateRepository.filteredTable.collect()

    assert(mockFilteredData.length == 1)
    assert(mockFilteredData.map(_.rate) sameElements Array(BigDecimal(0.9)))
  }

  "ReceiversRegistry table" should "be filtered on latest date" in {
    import sparkSession.implicits._

    // Mock transactions
    val receiversRegistry = Seq(
      ReceiversRegistry(
        "COMP_001",
        "ES",
        "2025-01-25"
      ),
      ReceiversRegistry(
        "COMP_002",
        "US",
        "2024-11-25"
      ),
      ReceiversRegistry(
        "COMP_003",
        "UK",
        "2025-01-25"
      )
    ).toDF()

    // Mock connector
    val mockConnector = new PostgresConnector(config.getConfig("app.postgres")) {
      override def readTable(tableFQN: String, schema: StructType): DataFrame = receiversRegistry
    }

    val mockReceiversRegistryRepository = PostgresReceiversRegistryRepository(mockConnector, date)

    val mockFilteredData = mockReceiversRegistryRepository.filteredTable.collect()

    assert(mockFilteredData.length == 2)
    assert(mockFilteredData.map(_.receiver_account) sameElements Array("COMP_001", "COMP_003"))
  }
}
