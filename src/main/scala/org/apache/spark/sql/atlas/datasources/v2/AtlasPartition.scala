package org.apache.spark.sql.atlas.datasources.v2

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConversions._

class AtlasPartition(dataSchema: StructType, requiredSchema: StructType,
    blockId: Long, blockLength: Long, locations: Array[String])
    extends InputPartition[InternalRow] {
  override def preferredLocations: Array[String] = locations

  override def createPartitionReader: InputPartitionReader[InternalRow] = {
    println("AtlasPartition.createPartitionReader for block "
      + blockId + " with length " + blockLength)
    new AtlasPartitionReader(dataSchema, requiredSchema,
      blockId, blockLength, locations)
  }
}
