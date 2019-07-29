package org.apache.spark.sql.atlas.datasources.v2

import com.bushpath.anamnesis.ipc.rpc.RpcClient

import org.apache.hadoop.hdfs.protocol.proto.{ClientNamenodeProtocolProtos, HdfsProtos}

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType;

import com.bushpath.atlas.spark.sql.util.Parser

import java.util.{ArrayList, HashSet, List};

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class AtlasSourceReader(blocks: Map[Long, HdfsProtos.LocatedBlockProto],
    dataSchema: StructType) extends DataSourceReader
    with SupportsPushDownFilters with SupportsPushDownRequiredColumns {
  private var requiredSchema = dataSchema

  override def readSchema: StructType = requiredSchema

  override def planInputPartitions
      : List[InputPartition[InternalRow]] = {
    // TODO - change hosts from ipAddr:port to datanodeUuid
    var hosts: Map[String, HashSet[Long]] = Map()

    // process block locations
    for (lbProto <- blocks.values) {
      // parse block id
      val blockId = lbProto.getB.getBlockId

      // parse locations
      for (diProto <- lbProto.getLocsList) {
        val didProto = diProto.getId
        val address = didProto.getIpAddr + ":" + didProto.getXferPort

        val set = hosts.get(address) match {
          case None => {
            val set: HashSet[Long] = new HashSet
            hosts += (address -> set)
            set
          };
          case Some(set) => {
            set
          };
        }

        set.add(blockId)
      }
    }

    // compute partitions
    for (blockId <- blocks.keys) {
      // find host containing block with shortest blockId list
      var blockHostOption: Option[String] = None
      var blockHostLength = Integer.MAX_VALUE
      for (host <- hosts.keys) {
        val set = hosts(host)
        if (set.contains(blockId)) {
          if (blockHostOption == None || set.size < blockHostLength) {
            blockHostOption = Some(host)
            blockHostLength = set.size
          }
        }
      }

      // remove block from all other hosts
      val blockHost = blockHostOption.orNull
      for (host <- hosts.keys) {
        if (host != blockHost) {
          val set = hosts(host)
          set.remove(blockId)
        }
      }
    }

    // compile InputPartitions
    val partitions: List[InputPartition[InternalRow]] = new ArrayList()
    for (host <- hosts.keys) {
      // if host contains blocks -> create partition
      val set = hosts(host)
      if (set.size != 0) {
        var partitionBlocks =
          new ListBuffer[HdfsProtos.LocatedBlockProto]()
        for (blockId <- set) {
          partitionBlocks += blocks(blockId)
        }

        partitions += new AtlasPartition(dataSchema,
          requiredSchema, partitionBlocks)
      }
    }

    partitions
  }

  override def pruneColumns(requiredSchema: StructType) = {
    this.requiredSchema = requiredSchema
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // TODO
    println("TODO - handle " + filters.length
      + " filters: " + filters.toList)

    filters
  }

  override def pushedFilters: Array[Filter] = {
    Array()
  }
}
