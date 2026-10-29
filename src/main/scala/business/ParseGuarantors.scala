package com.alitariverdy
package business

import data.rows.{Transaction, TransitoryRow}
import utils.JsonGuarantorParser

import org.apache.spark.sql.{Dataset, Encoder}

object ParseGuarantors {
  def apply(
      df: Dataset[Transaction]
  )(implicit etr: Encoder[TransitoryRow]): Dataset[TransitoryRow] =
    df.flatMap { txn =>
      JsonGuarantorParser.parseJson(txn.guarantors).toOption match {
        case Some(guarantors) =>
          guarantors.map(g => txn.convertToTransitoryRow(g.name, g.percentage))
        case None => throw new IllegalArgumentException(s"Invalid guarantors in transaction ${txn.transaction_id}")
      }
    }
}
