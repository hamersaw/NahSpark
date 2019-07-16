package org.apache.spark.sql.atlas

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.types.UDTRegistration
import org.locationtech.jts.geom.Geometry

import org.apache.spark.sql.atlas.expressions._

object AtlasRegister {
  final val expressions:Seq[FunctionBuilder] = Seq(
    BuildPoint,
    // DoubleExpressions
    Area,
    Distance,
    Length
  )

  def init(sparkSession: SparkSession) = {
    // register AtlasGeometryUDT
    UDTRegistration.register(classOf[Geometry].getName,
      classOf[AtlasGeometryUDT].getName)

    // register expressions
    val functionRegistry = sparkSession.sessionState.functionRegistry
    for (expression <- this.expressions) {
      functionRegistry.createOrReplaceTempFunction(
        expression.getClass.getSimpleName.dropRight(1), expression)
    }
  }
}
