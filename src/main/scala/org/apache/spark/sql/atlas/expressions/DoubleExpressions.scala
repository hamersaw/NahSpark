package org.apache.spark.sql.atlas.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{DataType, DoubleType}

import com.bushpath.atlas.spark.sql.util.Converter;

abstract class DoubleExpression(inputExpressions: Seq[Expression])
    extends Expression with CodegenFallback with Serializable {
  override def dataType: DataType = DoubleType

  override def nullable: Boolean = false

  override def children: Seq[Expression] = inputExpressions
}

case class Area(inputExpressions: Seq[Expression])
    extends DoubleExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometry = Converter.toGeometry(inputExpressions(0).eval(input))
    geometry.getArea()
  }
}

case class Distance(inputExpressions: Seq[Expression])
    extends DoubleExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometryOne = Converter.toGeometry(inputExpressions(0).eval(input))
    val geometryTwo = Converter.toGeometry(inputExpressions(1).eval(input))
    geometryOne.distance(geometryTwo)
  }
}

case class Length(inputExpressions: Seq[Expression])
    extends DoubleExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometry = Converter.toGeometry(inputExpressions(0).eval(input))
    geometry.getLength()
  }
}
