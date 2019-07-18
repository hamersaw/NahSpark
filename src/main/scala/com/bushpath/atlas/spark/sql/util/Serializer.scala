package com.bushpath.atlas.spark.sql.util

import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.locationtech.jts.geom.{Coordinate, CoordinateSequence, Geometry, GeometryFactory, LinearRing, LineString, Point, Polygon}
import org.locationtech.jts.geom.impl.CoordinateArraySequence
import java.io.{BufferedInputStream, BufferedOutputStream, ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

object Serializer {
  final val lineString = 0x01
  final val point = 0x02
  final val polygon = 0x03

  def deserialize(arrayData: ArrayData): Geometry = {
    // intialize input streams
    val byteIn = new ByteArrayInputStream(arrayData.toByteArray)
    val bufIn = new BufferedInputStream(byteIn)
    val in = new DataInputStream(bufIn)

    // serialize geometry
    val geometry = this.deserialize(in)

    // close input streams
    in.close
    bufIn.close
    byteIn.close

    geometry
  }

  def deserialize(in: DataInputStream): Geometry = {
    val geometryType = in.read().asInstanceOf[Byte]
    geometryType match {
      case this.lineString => {
        // deserialize line string
        val coordinateSequence = deserializeCoordinateSequence(in)
        new LineString(coordinateSequence, GeometryUtil.factory)
      };
      case this.point => {
        // deserialize point
        val coordinateSequence = deserializeCoordinateSequence(in)
        new Point(coordinateSequence, GeometryUtil.factory)
      };
      case this.polygon => {
        // deserialize polygon
        val coordinateSequence = deserializeCoordinateSequence(in)
        val exteriorRing = new LinearRing(coordinateSequence,
          GeometryUtil.factory)

        val numInteriorRing = in.readInt
        val interiorRingArray = new Array[LinearRing](numInteriorRing)
        for (i <- 0 to numInteriorRing) {
          val coordinateSequence = deserializeCoordinateSequence(in)
          val interiorRing = new LinearRing(coordinateSequence,
            GeometryUtil.factory)

          interiorRingArray(i) = interiorRing
        }

        new Polygon(exteriorRing, interiorRingArray, GeometryUtil.factory)
      };
    }
  }

  private def deserializeCoordinateSequence(in: DataInputStream)
      : CoordinateSequence = {
    val numPoints = in.readInt
    val coordinateSequence = new CoordinateArraySequence(numPoints)
    for (i <- 0 to (numPoints - 1)) {
      val x = in.readDouble
      coordinateSequence.setOrdinate(i, 0, x)

      val y = in.readDouble
      coordinateSequence.setOrdinate(i, 1, y)
    }

    coordinateSequence
  }

  def serialize(geometry: Geometry): ArrayData = {
    // TODO determine geometry serialized size
    /*val byteCount = obj match {
      case lineString: LineString => 1 + (obj.getNumPoints() * 2 * 8);
      case point: Point => 1 + (obj.getNumPoints() * 2 * 8);
      case polygon: Polygon => 1 + (3 * 4) + (obj.getNumPoints() * 2 * 8);
    }
    //val byteOut = new ByteArrayOutputStream(byteCount)*/

    // intialize output streams
    val byteOut = new ByteArrayOutputStream()
    val bufOut = new BufferedOutputStream(byteOut)
    val out = new DataOutputStream(bufOut)

    // serialize geometry
    this.serialize(geometry, out)

    // close output streams
    out.close
    bufOut.close
    byteOut.close

    // return byte array
    new GenericArrayData(byteOut.toByteArray)
  }

  def serialize(geometry: Geometry, out: DataOutputStream): Unit = {
    geometry match {
      case lineString: LineString => {
        // serialize line string
        out.write(this.lineString)
        out.writeInt(lineString.getNumPoints)
        for (i <- 0 to lineString.getNumPoints) {
          val point = lineString.getPointN(i)
          out.writeDouble(point.getX)
          out.writeDouble(point.getY)
        }
      };
      case point: Point => {
        // serialize point
        out.write(this.point)
        out.writeInt(1)
        out.writeDouble(point.getX)
        out.writeDouble(point.getY)
      };
      case polygon: Polygon => {
        // serialize polygon
        out.write(this.polygon)
        serialize(polygon.getExteriorRing, out)
        out.writeInt(polygon.getNumInteriorRing)
        for (i <- 0 to polygon.getNumInteriorRing) {
          serialize(polygon.getInteriorRingN(i), out)
        }
      };
    }
  }
}