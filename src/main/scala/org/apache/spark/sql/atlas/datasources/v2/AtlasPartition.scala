package org.apache.spark.sql.atlas.datasources.v2

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType;

import scala.collection.JavaConversions._

class AtlasPartition(schema: StructType,
    blocks: Seq[HdfsProtos.LocatedBlockProto])
    extends InputPartition[InternalRow] {
  override def preferredLocations: Array[String] = {
    var blocksPerHost = Map[String, Int]()
    for (lbProto <- blocks) {
      for (diProto <- lbProto.getLocsList) {
        val ipAddr = diProto.getId.getIpAddr
        blocksPerHost.get(ipAddr) match {
          case None => {
            blocksPerHost += (ipAddr -> 1)
          };
          case Some(x) => {
            blocksPerHost += (ipAddr -> (x + 1))
          }
        }
      }
    }

    // TODO - check if preferred locations is working
    //println("PREFFERRED LOCATIONS: " + blocksPerHost.toSeq.sortWith(_._1 > _._1).map(x => x._1))
    blocksPerHost.toSeq.sortWith(_._1 > _._1).map(x => x._1).toArray
  }

  override def createPartitionReader
      : InputPartitionReader[InternalRow] = {
    new AtlasPartitionReader(schema, blocks)
  }
}
