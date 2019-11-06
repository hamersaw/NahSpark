package org.apache.spark.sql.nah.datasources.v2

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConversions._

class NahPartition(dataSchema: StructType, requiredSchema: StructType,
    blockId: Long, blockLength: Long, locations: Array[String])
    extends InputPartition[InternalRow] {
  override def preferredLocations: Array[String] = locations

  override def createPartitionReader: InputPartitionReader[InternalRow] = {
    new NahPartitionReader(dataSchema, requiredSchema,
      blockId, blockLength, locations)
  }
}
