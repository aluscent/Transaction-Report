package com.alitariverdy
package utils

import java.time.{LocalDate, ZoneOffset}

object GetDate {
  def apply(args: Array[String]): String =
    if (args.nonEmpty) LocalDate.parse(args(0)).toString
    else LocalDate.now(ZoneOffset.UTC).toString
}
