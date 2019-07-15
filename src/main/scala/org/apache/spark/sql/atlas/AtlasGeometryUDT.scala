package org.apache.spark.sql.atlas

import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.{Geometry, LineString, Point, Polygon}

import com.bushpath.atlas.spark.sql.util.Serializer

import java.io.{BufferedInputStream, BufferedOutputStream, ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

private[sql] class AtlasGeometryUDT extends UserDefinedType[Geometry] {
  override def deserialize(datum: Any): Geometry = {
    datum match {
      case arrayData: ArrayData => {
        Serializer.deserialize(arrayData)
      }
    }
  }

  override def serialize(obj: Geometry): Any = {
    Serializer.serialize(obj)
  }

  override def sqlType: DataType = ArrayType(ByteType)

  override def userClass: Class[Geometry] = classOf[Geometry]
}
