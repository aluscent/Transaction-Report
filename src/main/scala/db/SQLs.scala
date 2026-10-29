package com.alitariverdy
package db

object SQLs {
  // Conversion and allocation are shared with the Dataset path; SQL is the
  // alternative aggregation, rather than a second interpretation of financial rules.
  val sqlReport: String => String = (reportDate: String) => s"""
      |SELECT guarantor_name, country, '$reportDate' AS partition_date,
      |  COALESCE(class_a_sum, 0) AS class_a_sum, COALESCE(class_a_avg, 0) AS class_a_avg,
      |  COALESCE(class_b_sum, 0) AS class_b_sum, COALESCE(class_b_avg, 0) AS class_b_avg,
      |  COALESCE(class_c_sum, 0) AS class_c_sum, COALESCE(class_c_avg, 0) AS class_c_avg,
      |  COALESCE(class_d_sum, 0) AS class_d_sum, COALESCE(class_d_avg, 0) AS class_d_avg
      |FROM (SELECT guarantor_name, country, txn_type, amount FROM allocated_transactions)
      |PIVOT (
      |  SUM(amount) AS sum, AVG(amount) AS avg
      |  FOR txn_type IN ('CLASS_A' AS class_a, 'CLASS_B' AS class_b,
      |                   'CLASS_C' AS class_c, 'CLASS_D' AS class_d)
      |)
      """.stripMargin
}
