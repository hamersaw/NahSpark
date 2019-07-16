package org.apache.spark.sql.atlas.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{DataType, DoubleType}

import com.bushpath.atlas.spark.sql.util.Serializer;

abstract class DoubleExpression(inputExpressions: Seq[Expression])
    extends Expression with CodegenFallback with Serializable {
  override def dataType: DataType = DoubleType

  override def nullable: Boolean = false;

  override def children: Seq[Expression] = inputExpressions;
}

case class Area(inputExpressions: Seq[Expression])
    extends DoubleExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val array = inputExpressions(0).eval(input).asInstanceOf[ArrayData]
    val geometry = Serializer.deserialize(array)
    geometry.getArea()
  }
}

case class Distance(inputExpressions: Seq[Expression])
    extends DoubleExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val arrayOne = inputExpressions(0).eval(input).asInstanceOf[ArrayData]
    val arrayTwo = inputExpressions(1).eval(input).asInstanceOf[ArrayData]

    val geometryOne = Serializer.deserialize(arrayOne)
    val geometryTwo = Serializer.deserialize(arrayTwo)
    geometryOne.distance(geometryTwo)
  }
}

case class Length(inputExpressions: Seq[Expression])
    extends DoubleExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val array = inputExpressions(0).eval(input).asInstanceOf[ArrayData]
    val geometry = Serializer.deserialize(array)
    geometry.getLength()
  }
}
