package com.alitariverdy

import business.{EuroExchangeRates, ParseGuarantors, PivotTransactionType}
import data.rows.{ExchangeRate, Guarantee, Transaction}
import db.SQLs
import utils.JsonGuarantorParser
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import java.sql.Date

class FinancialRulesSpec extends AnyFunSuite with BeforeAndAfterAll {
  implicit lazy val spark: SparkSession = SparkSession.builder().master("local[2]")
    .appName("financial-rules-test").config("spark.driver.bindAddress", "127.0.0.1").config("spark.driver.host", "127.0.0.1").config("spark.ui.enabled", "false").getOrCreate()
  override def afterAll(): Unit = spark.stop()
  private def transaction(id: String = "t", currency: String = "USD", amount: String = "10.01") =
    Transaction(id, """[{"NAME":"Alice","PERCENTAGE":0.1},{"NAME":"Bob","PERCENTAGE":0.9}]""",
      "sender", "receiver", "CLASS_A", "GB", currency, BigDecimal(amount), "OPEN",
      Date.valueOf("2025-01-01"), "2025-01-01")
  private def rate(value: String = "0.8") = ExchangeRate("USD", "EUR", BigDecimal(value), "2025-01-01")

  test("report date rejects malformed values before SQL interpolation") {
    assert(utils.GetDate(Array("2025-02-25")) == "2025-02-25")
    intercept[java.time.format.DateTimeParseException] { utils.GetDate(Array("2025-02-30")) }
    intercept[java.time.format.DateTimeParseException] { utils.GetDate(Array("x' OR 1=1")) }
  }
  test("decimal allocation preserves exact shares and rejects invalid ownership") {
    assert(JsonGuarantorParser.parseJson(transaction().guarantors).toOption.get.head.percentage == BigDecimal("0.1"))
    assert(transaction().convertToTransitoryRow("Alice", BigDecimal("0.1")).amount == BigDecimal("1.001"))
    for (shares <- Seq("[]", """[{"NAME":"A","PERCENTAGE":1.1}]""",
      """[{"NAME":"A","PERCENTAGE":-0.1}]""",
      """[{"NAME":"A","PERCENTAGE":0.8},{"NAME":"B","PERCENTAGE":0.3}]""")) {
      assert(JsonGuarantorParser.parseJson(shares).isLeft)
    }
  }
  test("conversion preserves cardinality and sets the resulting currency") {
    import spark.implicits._
    val rows = EuroExchangeRates.convert(Seq(transaction(), transaction("t2")).toDS(), Seq(rate()).toDS()).collect()
    assert(rows.length == 2)
    assert(rows.forall(r => r.currency == "EUR" && r.amount == BigDecimal("8.008")))
  }
  test("missing, duplicate and nonpositive rates fail instead of losing or multiplying transactions") {
    import spark.implicits._
    val txns = Seq(transaction()).toDS()
    for (rates <- Seq(Seq.empty[ExchangeRate], Seq(rate(), rate()), Seq(rate("0")), Seq(rate("-1")))) {
      intercept[IllegalArgumentException] { EuroExchangeRates.convert(txns, rates.toDS()) }
    }
  }
  test("SQL and Dataset aggregations agree with independent expected totals") {
    import spark.implicits._
    val euros = EuroExchangeRates.convert(Seq(transaction()).toDS(), Seq(rate()).toDS())
    val allocated = ParseGuarantors(euros)
    allocated.createOrReplaceTempView("allocated_transactions")
    val api = PivotTransactionType("2025-01-01", allocated).collect().sortBy(_.guarantor_name)
    val sql = spark.sql(SQLs.sqlReport("2025-01-01")).as[Guarantee].collect().sortBy(_.guarantor_name)
    assert(api.toSeq == sql.toSeq)
    assert(api.map(_.class_a_sum).toSeq == Seq(BigDecimal("0.8008"), BigDecimal("7.2072")))
    assert(api.forall(r => r.class_a_sum == r.class_a_avg && r.class_b_sum == 0 && r.class_d_avg == 0))
  }
}
