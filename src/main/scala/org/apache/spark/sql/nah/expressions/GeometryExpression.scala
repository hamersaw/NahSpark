package org.apache.spark.sql.nah.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType

import com.bushpath.nah.spark.sql.util.Converter;
import org.apache.spark.sql.nah.NahGeometryUDT

abstract class GeometryExpression(inputExpressions: Seq[Expression])
    extends Expression with CodegenFallback with Serializable {
  override def dataType: DataType = new NahGeometryUDT()

  override def nullable: Boolean = false

  override def children: Seq[Expression] = inputExpressions
}

case class Buffer(inputExpressions: Seq[Expression])
    extends GeometryExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometry = Converter.toGeometry(inputExpressions(0).eval(input))
    val distance = Converter.toDouble(inputExpressions(1).eval(input))
    geometry.buffer(distance)
  }
}

case class Difference(inputExpressions: Seq[Expression])
    extends GeometryExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometryOne = Converter.toGeometry(inputExpressions(0).eval(input))
    val geometryTwo = Converter.toGeometry(inputExpressions(1).eval(input))
    geometryOne.difference(geometryTwo)
  }
}

case class ConvexHull(inputExpressions: Seq[Expression])
    extends GeometryExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometry = Converter.toGeometry(inputExpressions(0).eval(input))
    geometry.convexHull
  }
}

case class Envelope(inputExpressions: Seq[Expression])
    extends GeometryExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometry = Converter.toGeometry(inputExpressions(0).eval(input))
    geometry.getEnvelope
  }
}

case class Intersection(inputExpressions: Seq[Expression])
    extends GeometryExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometryOne = Converter.toGeometry(inputExpressions(0).eval(input))
    val geometryTwo = Converter.toGeometry(inputExpressions(1).eval(input))
    geometryOne.intersection(geometryTwo)
  }
}

case class Normalize(inputExpressions: Seq[Expression])
    extends GeometryExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometry = Converter.toGeometry(inputExpressions(0).eval(input))
    geometry.norm
  }
}

case class SymDifference(inputExpressions: Seq[Expression])
    extends GeometryExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometryOne = Converter.toGeometry(inputExpressions(0).eval(input))
    val geometryTwo = Converter.toGeometry(inputExpressions(1).eval(input))
    geometryOne.symDifference(geometryTwo)
  }
}

case class Union(inputExpressions: Seq[Expression])
    extends GeometryExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometryOne = Converter.toGeometry(inputExpressions(0).eval(input))
    val geometryTwo = Converter.toGeometry(inputExpressions(1).eval(input))
    geometryOne.union(geometryTwo)
  }
}
