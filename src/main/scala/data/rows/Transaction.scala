package com.alitariverdy
package data.rows

import data.TableRow

import java.sql.Date

case class Transaction(
    transaction_id: String,
    guarantors: String,
    sender_name: String,
    receiver_name: String,
    txn_type: String,
    country: String,
    currency: String,
    amount: BigDecimal,
    status: String,
    initiation_date: Date,
    partition_date: String
) extends TableRow {

  def convertToTransitoryRow(guarantorName: String, guarantorPercentage: BigDecimal): TransitoryRow =
    TransitoryRow(
      country,
      currency,
      txn_type,
      partition_date,
      amount * guarantorPercentage,
      guarantorName
    )
}
