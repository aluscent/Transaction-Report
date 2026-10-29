package com.alitariverdy

import business.{CheckRegisteredReceivers, EuroExchangeRates, ParseGuarantors, PivotTransactionType}
import data.rows.{Transaction, TransitoryRow}
import db.PostgresConnector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Date

class BusinessSpec extends AnyFlatSpec with Report with BeforeAndAfterAll {

  val date: String = "2025-02-25"

  override def beforeAll(): Unit = {
    sparkSession


  }

  override def afterAll(): Unit = {
    sparkSession.stop()
  }

  // Pivot tests
  "transactions" should "correctly pivot into guarantee aggregates" in {
    import sparkSession.implicits._

    // Test data setup
    val testData = Seq(
      TransitoryRow("US", "EUR", "CLASS_A", "2025-01-01", BigDecimal(100), "G_001"),
      TransitoryRow("US", "EUR", "CLASS_B", "2025-01-01", BigDecimal(200), "G_002"),
      TransitoryRow("US", "EUR", "CLASS_B", "2025-01-01", BigDecimal(300), "G_003"),
      TransitoryRow("US", "EUR", "CLASS_B", "2025-01-01", BigDecimal(100), "G_003"),
      TransitoryRow("US", "EUR", "CLASS_C", "2025-01-01", BigDecimal(400), "G_001"),
    )

    val inputDS = sparkSession.createDataset(testData)

    val resultDS = PivotTransactionType(date, inputDS)
    val results = resultDS.collect().sortBy(_.guarantor_name)

    // Assert G1 aggregates
    val g1Result = results.find(_.guarantor_name == "G_001").get
    assert(g1Result.class_a_sum == 100)
    assert(g1Result.class_a_avg == 100)
    assert(g1Result.class_b_sum == 0)
    assert(g1Result.class_c_sum == 400)
    assert(g1Result.class_c_avg == 400)
    assert(g1Result.class_d_sum == 0)

    // Assert G3 aggregates
    val g2Result = results.find(_.guarantor_name == "G_003").get
    assert(g2Result.class_a_sum == 0)
    assert(g2Result.class_b_sum == 400)
    assert(g2Result.class_b_avg == 200)
    assert(g2Result.class_c_sum == 0)
    assert(g2Result.class_d_sum == 0)
  }

  "missing txn_types" should "fill with zeros" in {
    import sparkSession.implicits._

    val testData = Seq(
      TransitoryRow("US", "EUR", "CLASS_D", "2025-01-01", BigDecimal(500), "G_003")
    )

    val inputDS = sparkSession.createDataset(testData)
    val result = PivotTransactionType(date, inputDS).first()

    assert(result.class_a_sum == 0)
    assert(result.class_b_sum == 0)
    assert(result.class_c_sum == 0)
    assert(result.class_d_sum == 500)
  }

  // Parsing tests
  "Valid guarantors" should "parse into TransitoryRows" in {
    import sparkSession.implicits._

    // Test data - valid JSON with 2 guarantors
    val validTransaction = Transaction("TX1",
      """[{"NAME": "G_001", "PERCENTAGE": 0.5}, {"NAME": "G_002", "PERCENTAGE": 0.3}]""",
      "Sender1", "Receiver1", "CLASS_A", "US", "USD", BigDecimal(100),
      "OPEN", Date.valueOf("2025-02-15"), "2025-02-20"
    )

    val inputDS = sparkSession.createDataset(Seq(validTransaction))

    val resultDS = ParseGuarantors(inputDS)
    val results = resultDS.collect()

    assert(results.length == 2)
    assert(results.exists(_.guarantor_name == "G_001" &&
      (results.head.amount.toDouble / 100) == 0.5))
    assert(results.exists(_.guarantor_name == "G_002" &&
      (results.last.amount.toDouble / 100) == 0.3))
  }

  "For invalid guarantors" should "fail the batch" in {
    import sparkSession.implicits._

    // Test data - invalid JSON structure
    val invalidTransaction = Transaction("TX2",
      """[{"INVALID_KEY": "G1"}]""", // Missing required fields
      "Sender2", "Receiver2", "CLASS_B", "EU", "EUR", BigDecimal(200),
      "OPEN", Date.valueOf("2025-02-15"), "2025-02-20"
    )

    val inputDS = sparkSession.createDataset(Seq(invalidTransaction))

    val resultDS = ParseGuarantors(inputDS)

    intercept[Exception] { resultDS.collect() }
  }

  // CheckRegisteredReceivers tests
  "transactions with unregistered receivers" should "be filtered" in {
    import sparkSession.implicits._

    // Mock transactions
    val transactions = Seq(
      Transaction("TX1", """[{"NAME": "G_001", "PERCENTAGE": 0.5}]""",
        "Sender1", "Receiver1", "CLASS_A", "US", "EUR", BigDecimal(100),
        "OPEN", Date.valueOf("2025-02-15"), "2025-02-20"
      ),
      Transaction("TX2", """[{"NAME": "G_002", "PERCENTAGE": 0.3}]""",
        "Sender2", "Receiver2", "CLASS_B", "ES", "EUR", BigDecimal(200),
        "OPEN", Date.valueOf("2025-02-15"), "2025-02-20"
      )
    ).toDS()

    // Mock registry (only Receiver1/US exists)
    val receiversRegistry = Seq(
      ("Receiver1", "US", "2025-02-20"),
      ("Receiver3", "MX", "2025-02-20")
    ).toDF("receiver_account", "country", "partition_date")

    // Mock connector
    val mockConnector = new PostgresConnector(config.getConfig("app.postgres")) {
      override def readTable(tableFQN: String, schema: StructType): DataFrame = receiversRegistry
    }

    val result = CheckRegisteredReceivers(transactions, mockConnector, date)

    assert(result.count() == 1)
    assert(result.first().receiver_name == "Receiver1")
  }

  // EuroExchangeRates tests
  "amounts" should "convert using exchange rates" in {
    import sparkSession.implicits._

    // Mock transactions
    val transactions = Seq(
      Transaction("TX1", """[{"NAME": "G_001", "PERCENTAGE": 0.5}]""",
        "Sender1", "Receiver1", "CLASS_A", "US", "USD", BigDecimal(100),
        "OPEN", Date.valueOf("2025-02-15"), "2025-02-20"
      ),
      Transaction("TX2", """[{"NAME": "G_002", "PERCENTAGE": 0.3}]""",
        "Sender2", "Receiver2", "CLASS_B", "ES", "JPY", BigDecimal(200),
        "OPEN", Date.valueOf("2025-02-15"), "2025-02-20"
      )
    ).toDS()

    // Mock exchange rates
    val exchangeRates = Seq(
      ("USD", "EUR", BigDecimal(0.85), "2025-02-20"),
      ("JPY", "EUR", BigDecimal(0.0075), "2025-02-20")
    ).toDF("from_currency", "to_currency", "rate", "partition_date")

    // Mock connector
    val mockConnector = new PostgresConnector(config.getConfig("app.postgres")) {
      override def readTable(tableFQN: String, schema: StructType): DataFrame = exchangeRates
    }

    val result = EuroExchangeRates(transactions, mockConnector, date)
    val results = result.collect()

    assert(results(0).amount == BigDecimal(85.0))  // USD: 100 * 0.85
    assert(results(1).amount == BigDecimal(1.5))   // JPY: 200 * 0.0075
  }
}
