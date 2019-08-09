package org.apache.spark.sql.atlas.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{GeometryFactory, LinearRing, LineString, Point, Polygon}
import org.locationtech.jts.geom.impl.CoordinateArraySequence

import com.bushpath.atlas.spark.sql.util.{Converter, GeometryUtil, Serializer}
import org.apache.spark.sql.atlas.AtlasGeometryUDT

abstract class BuildExpression(inputExpressions: Seq[Expression])
    extends Expression with CodegenFallback with Serializable {
  override def dataType: DataType = new AtlasGeometryUDT()

  override def nullable: Boolean = false

  override def children: Seq[Expression] = inputExpressions
}

/*case class BuildGeometryFromWkt(inputExpressions: Seq[Expression])
    extends Expression with CodegenFallback {
  override def dataType: DataType = new AtlasGeometryUDT()

  override def eval(input: InternalRow): Any = {
    null // TODO
  }

  override def nullable: Boolean = false;

  override def children: Seq[Expression] = inputExpressions;
}*/

case class BuildPoint(inputExpressions: Seq[Expression])
    extends BuildExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val x = Converter.toDouble(inputExpressions(0).eval(input))
    val y = Converter.toDouble(inputExpressions(1).eval(input))

    var coordinateSequence = new CoordinateArraySequence(1)
    coordinateSequence.setOrdinate(0, 0, x);
    coordinateSequence.setOrdinate(0, 1, y);
    var point = new Point(coordinateSequence, GeometryUtil.factory);
    Serializer.serialize(point)
  }
}

case class BuildLine(inputExpressions: Seq[Expression])
    extends BuildExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val pointCount = inputExpressions.size / 2

    var coordinateSequence = new CoordinateArraySequence(pointCount)
    for (i <- 1 to pointCount) {
      val index = (i - 1) * 2
      val x = Converter.toDouble(inputExpressions(index).eval(input))
      val y = Converter.toDouble(inputExpressions(index + 1).eval(input))

      coordinateSequence.setOrdinate(i - 1, 0, x)
      coordinateSequence.setOrdinate(i - 1, 1, x)
    }

    val line = new LineString(coordinateSequence, GeometryUtil.factory)
    Serializer.serialize(line)
  }
}

case class BuildPolygon(inputExpressions: Seq[Expression])
    extends BuildExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val pointCount = inputExpressions.size / 2

    var coordinateSequence = new CoordinateArraySequence(pointCount)
    for (i <- 1 to pointCount) {
      val index = (i - 1) * 2
      val x = Converter.toDouble(inputExpressions(index).eval(input))
      val y = Converter.toDouble(inputExpressions(index + 1).eval(input))

      coordinateSequence.setOrdinate(i - 1, 0, x)
      coordinateSequence.setOrdinate(i - 1, 1, y)
    }

    val linearRing = new LinearRing(coordinateSequence, GeometryUtil.factory)
    val holes: Array[LinearRing] = Array()
    val polygon = new Polygon(linearRing, holes, GeometryUtil.factory)
    Serializer.serialize(polygon)
  }
}
