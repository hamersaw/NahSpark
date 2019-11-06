package com.bushpath.nah.spark.sql.util

import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.Decimal
import org.locationtech.jts.geom.Geometry

object Converter {
  def toDouble(value: Any): Double = {
    value match {
      case decimal: Decimal => decimal.toDouble;
      case double: Double => double;
      case int: Integer => int.toDouble;
    }
  }

  def toGeometry(value: Any): Geometry = {
    value match {
      case arrayData: ArrayData => Serializer.deserialize(arrayData);
    }
  }
}
