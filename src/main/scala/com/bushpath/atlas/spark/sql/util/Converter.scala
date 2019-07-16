package com.bushpath.atlas.spark.sql.util

import org.apache.spark.sql.types.Decimal

object Converter {
  def toDouble(value: Any): Double = {
    value match {
      case decimal: Decimal => decimal.toDouble;
      case double: Double => double;
    }
  }
}
