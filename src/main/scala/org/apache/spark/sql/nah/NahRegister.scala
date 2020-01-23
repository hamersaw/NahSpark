package org.apache.spark.sql.nah

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.types.UDTRegistration
import org.locationtech.jts.geom.Geometry

import org.apache.spark.sql.nah.expressions._

object NahRegister {
  final val expressions:Seq[FunctionBuilder] = Seq(
    // BooleanExpressions
    Contains,
    Covers,
    Crosses,
    Disjoint,
    Equals,
    EqualsTollerance,
    Intersects,
    IsEmpty,
    IsSimple,
    IsValid,
    Overlaps,
    Touches,
    Within,
    // BuildExpressions
    BuildPoint,
    BuildPolygon,
    // DoubleExpressions
    Area,
    Distance,
    Length,
    // GeometryExpressions
    Buffer,
    ConvexHull,
    Difference,
    Envelope,
    Intersection,
    Normalize,
    SymDifference,
    Union,
    // IntegerExpressions
    Dimension,
    NumPoints
  )

  def init(sparkSession: SparkSession) = {
    // register NahGeometryUDT
    UDTRegistration.register(classOf[Geometry].getName,
      classOf[NahGeometryUDT].getName)

    // register expressions
    val functionRegistry = sparkSession.sessionState.functionRegistry
    for (expression <- this.expressions) {
      functionRegistry.createOrReplaceTempFunction(
        expression.getClass.getSimpleName.dropRight(1), expression)
    }

    // register filter injection optimization
    sparkSession.experimental.extraOptimizations =
      Seq(NahFilterInjectionOptimizationRule)
  }
}
