package org.apache.spark.sql.nah.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{BooleanType, DataType}

import com.bushpath.nah.spark.sql.util.Converter;

abstract class BooleanExpression(inputExpressions: Seq[Expression])
    extends Expression with CodegenFallback with Serializable {
  override def dataType: DataType = BooleanType

  override def nullable: Boolean = false

  override def children: Seq[Expression] = inputExpressions
}

case class Contains(inputExpressions: Seq[Expression])
    extends BooleanExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometryOne = Converter.toGeometry(inputExpressions(0).eval(input))
    val geometryTwo = Converter.toGeometry(inputExpressions(1).eval(input))
    geometryOne.contains(geometryTwo)
  }
}

case class Covers(inputExpressions: Seq[Expression])
    extends BooleanExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometryOne = Converter.toGeometry(inputExpressions(0).eval(input))
    val geometryTwo = Converter.toGeometry(inputExpressions(1).eval(input))
    geometryOne.covers(geometryTwo)
  }
}

case class Crosses(inputExpressions: Seq[Expression])
    extends BooleanExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometryOne = Converter.toGeometry(inputExpressions(0).eval(input))
    val geometryTwo = Converter.toGeometry(inputExpressions(1).eval(input))
    geometryOne.crosses(geometryTwo)
  }
}

case class Disjoint(inputExpressions: Seq[Expression])
    extends BooleanExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometryOne = Converter.toGeometry(inputExpressions(0).eval(input))
    val geometryTwo = Converter.toGeometry(inputExpressions(1).eval(input))
    geometryOne.disjoint(geometryTwo)
  }
}

case class Equals(inputExpressions: Seq[Expression])
    extends BooleanExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometryOne = Converter.toGeometry(inputExpressions(0).eval(input))
    val geometryTwo = Converter.toGeometry(inputExpressions(1).eval(input))
    geometryOne.equalsExact(geometryTwo)
  }
}

case class EqualsTollerance(inputExpressions: Seq[Expression])
    extends BooleanExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometryOne = Converter.toGeometry(inputExpressions(0).eval(input))
    val geometryTwo = Converter.toGeometry(inputExpressions(1).eval(input))
    val tollerance = Converter.toDouble(inputExpressions(2).eval(input))
    geometryOne.equalsExact(geometryTwo, tollerance)
  }
}

case class Intersects(inputExpressions: Seq[Expression])
    extends BooleanExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometryOne = Converter.toGeometry(inputExpressions(0).eval(input))
    val geometryTwo = Converter.toGeometry(inputExpressions(1).eval(input))
    geometryOne.intersects(geometryTwo)
  }
}

case class IsEmpty(inputExpressions: Seq[Expression])
    extends BooleanExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometry = Converter.toGeometry(inputExpressions(0).eval(input))
    geometry.isEmpty
  }
}

case class IsSimple(inputExpressions: Seq[Expression])
    extends BooleanExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometry = Converter.toGeometry(inputExpressions(0).eval(input))
    geometry.isSimple
  }
}

case class IsValid(inputExpressions: Seq[Expression])
    extends BooleanExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometry = Converter.toGeometry(inputExpressions(0).eval(input))
    geometry.isValid
  }
}

case class Overlaps(inputExpressions: Seq[Expression])
    extends BooleanExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometryOne = Converter.toGeometry(inputExpressions(0).eval(input))
    val geometryTwo = Converter.toGeometry(inputExpressions(1).eval(input))
    geometryOne.overlaps(geometryTwo)
  }
}

case class Touches(inputExpressions: Seq[Expression])
    extends BooleanExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometryOne = Converter.toGeometry(inputExpressions(0).eval(input))
    val geometryTwo = Converter.toGeometry(inputExpressions(1).eval(input))
    geometryOne.touches(geometryTwo)
  }
}

case class Within(inputExpressions: Seq[Expression])
    extends BooleanExpression(inputExpressions) with CodegenFallback {
  override def eval(input: InternalRow): Any = {
    val geometryOne = Converter.toGeometry(inputExpressions(0).eval(input))
    val geometryTwo = Converter.toGeometry(inputExpressions(1).eval(input))
    geometryOne.within(geometryTwo)
  }
}
