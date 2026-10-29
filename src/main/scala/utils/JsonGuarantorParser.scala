package com.alitariverdy
package utils

import data.rows.Guarantor

import scala.util.parsing.combinator._

object JsonGuarantorParser extends RegexParsers {
  // Basic tokens
  def stringLiteral: Parser[String] = "\"" ~> """[^"]*""".r <~ "\""
  def number: Parser[BigDecimal] = """-?\d+(\.\d+)?""".r ^^ { BigDecimal(_) }
  def ws: Parser[String] = """\s*""".r

  // JSON structure
  def obj: Parser[Guarantor] =
    "{" ~> ws ~>
      "\"NAME\"" ~> ws ~> ":" ~> ws ~> stringLiteral ~
      (ws ~> "," ~> ws ~> "\"PERCENTAGE\"" ~> ws ~> ":" ~> ws ~> number) <~
      ws <~ "}" ^^ {
      case name ~ percentage => Guarantor(name, percentage)
    }

  def array: Parser[List[Guarantor]] =
    "[" ~> ws ~> repsep(obj, ws ~> "," <~ ws) <~ ws <~ "]"

  def parseJson(input: String): Either[String, Seq[Guarantor]] =
    parseAll(array, input) match {
      case Success(result, _) =>
        if (result.isEmpty) Left("At least one guarantor is required")
        else if (result.exists(g => g.name.trim.isEmpty || g.percentage < 0 || g.percentage > 1))
          Left("Guarantor names must be non-empty and shares must be between 0 and 1")
        else if (result.map(_.name).distinct.size != result.size)
          Left("Duplicate guarantor names are not allowed")
        else if (result.map(_.percentage).sum > 1) Left("Guarantor shares exceed 1")
        else Right(result)
      case Failure(msg, _)    => Left(s"Parse failure: $msg")
      case Error(msg, _)      => Left(s"Parse error: $msg")
    }
}
