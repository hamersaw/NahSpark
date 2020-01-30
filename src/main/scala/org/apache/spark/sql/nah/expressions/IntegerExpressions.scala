package org.apache.spark.sql.nah.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, IntegerType}

import com.bushpath.nah.spark.sql.util.Converter;

abstract class IntegerExpression(inputExpressions: Seq[Expression])
    extends Expression with CodegenFallback with Serializable {
  override def dataType: DataType = IntegerType

  override def nullable: Boolean = false

  override def children: Seq[Expression] = inputExpressions
}

case class Dimension(inputExpressions: Seq[Expression])
    extends DoubleExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometry = Converter.toGeometry(inputExpressions(0).eval(input))
    geometry.getDimension
  }
}

case class NumPoints(inputExpressions: Seq[Expression])
    extends DoubleExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometry = Converter.toGeometry(inputExpressions(0).eval(input))
    geometry.getNumPoints
  }
}
