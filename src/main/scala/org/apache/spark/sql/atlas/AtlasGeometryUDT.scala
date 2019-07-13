package org.apache.spark.spark.sql.atlas;

import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry

private[sql] class AtlasGeometryUDT extends UserDefinedType[Geometry] {
  override def sqlType: DataType = ArrayType(ByteType)
  override def userClass: Class[Geometry] = classOf[Geometry]

  override def serialize(datum: Any): Geometry {
    // TODO
  }

  override def serialize(obj: Geometry): Any {
    // TODO
  }
}
