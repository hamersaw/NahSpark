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

object Constructors {
  final val geometryFactory = new GeometryFactory()
}

case class ST_GeometryFromWKT(inputExpressions: Seq[Expression])
    extends Expression with CodegenFallback {
  override def dataType: DataType = new AtlasGeometryUDT()

  override def eval(input: InternalRow): Any = {
    null // TODO
  }

  override def nullable: Boolean = false;

  override def children: Seq[Expression] = inputExpressions;
}

case class ST_Point(inputExpressions: Seq[Expression])
    extends Expression with CodegenFallback {
  override def dataType: DataType = new AtlasGeometryUDT()

  override def eval(input: InternalRow): Any = {
    //assert(inputExpressions.length == 2)
    //val x = inputExpressions(0).eval(input).asInstanceOf[Decimal].toDouble
    //val y = inputExpressions(1).eval(input).asInstanceOf[Decimal].toDouble
    val x = inputExpressions(0).eval(input).asInstanceOf[Double]
    val y = inputExpressions(1).eval(input).asInstanceOf[Double]

    var coordinateSequence = new CoordinateArraySequence(1)
    coordinateSequence.setOrdinate(0, 0, x);
    coordinateSequence.setOrdinate(0, 1, y);
    var point = new Point(coordinateSequence,
      Constructors.geometryFactory);
    return Serializer.serialize(point)
  }

  override def nullable: Boolean = false;

  override def children: Seq[Expression] = inputExpressions;
}
