package org.apache.spark.sql.atlas.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{GeometryFactory, Point}
import org.locationtech.jts.geom.impl.CoordinateArraySequence

import com.bushpath.atlas.spark.sql.util.Serializer;
import org.apache.spark.sql.atlas.AtlasGeometryUDT

object Builders {
  final val geometryFactory = new GeometryFactory()
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
    extends Expression with CodegenFallback {
  override def dataType: DataType = new AtlasGeometryUDT()

  override def eval(input: InternalRow): Any = {
    val x = inputExpressions(0).eval(input) match {
      case decimal: Decimal => decimal.toDouble;
      case double: Double => double;
    };

    val y = inputExpressions(1).eval(input) match {
      case decimal: Decimal => decimal.toDouble;
      case double: Double => double;
    };

    var coordinateSequence = new CoordinateArraySequence(1)
    coordinateSequence.setOrdinate(0, 0, x);
    coordinateSequence.setOrdinate(0, 1, y);
    var point = new Point(coordinateSequence,
      Builders.geometryFactory);
    return Serializer.serialize(point)
  }

  override def nullable: Boolean = false;

  override def children: Seq[Expression] = inputExpressions;
}
