package org.apache.spark.spark.sql.atlas;

import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.{Geometry, LineString, Point, Polygon}

import com.bushpath.atlas.spark.sql.util.Serializer

import java.io.{BufferedInputStream, BufferedOutputStream, ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

object GeometryType extends Enumeration {
  val lineString = Value(0x01)
  val point = Value(0x02)
  val polygon = Value(0x03)
}

private[sql] class AtlasGeometryUDT extends UserDefinedType[Geometry] {
  override def sqlType: DataType = ArrayType(ByteType)
  override def userClass: Class[Geometry] = classOf[Geometry]

  override def deserialize(datum: Any): Geometry = {
    datum match {
      case arrayData: ArrayData => {
        // intialize input streams
        val byteIn = new ByteArrayInputStream(arrayData.toByteArray)
        val bufIn = new BufferedInputStream(byteIn)
        val in = new DataInputStream(bufIn)

        // serialize geometry
        val geometry = Serializer.deserialize(in)

        // close input streams
        in.close
        bufIn.close
        byteIn.close

        geometry
      }
    }
  }

  override def serialize(obj: Geometry): Any = {
    // TODO determine geometry serialized size
    /*val byteCount = obj match {
      case lineString: LineString => 1 + (obj.getNumPoints() * 2 * 8);
      case point: Point => 1 + (obj.getNumPoints() * 2 * 8);
      case polygon: Polygon => 1 + (3 * 4) + (obj.getNumPoints() * 2 * 8);
    }*/
    //val byteOut = new ByteArrayOutputStream(byteCount)

    // intialize output streams
    val byteOut = new ByteArrayOutputStream()
    val bufOut = new BufferedOutputStream(byteOut)
    val out = new DataOutputStream(bufOut)

    // serialize geometry
    Serializer.serialize(obj, out)

    // close output streams
    out.close
    bufOut.close
    byteOut.close

    // return byte array
    new GenericArrayData(byteOut.toByteArray)
  }
}
