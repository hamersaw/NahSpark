package org.apache.spark.sql.atlas.datasources.v2

import com.bushpath.anamnesis.ipc.rpc.RpcClient

import org.apache.hadoop.hdfs.protocol.proto.{ClientNamenodeProtocolProtos, HdfsProtos}

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.{EqualTo, IsNotNull}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.{LongType, StringType, StructType};

import com.bushpath.atlas.spark.sql.util.Parser

import java.util.{ArrayList, HashSet, List};

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class AtlasSourceReader(blocks: Map[Long, HdfsProtos.LocatedBlockProto],
    dataSchema: StructType) extends DataSourceReader
    with SupportsPushDownFilters with SupportsPushDownRequiredColumns {
  private var requiredSchema = {
    var schema = StructType(dataSchema)
    schema = schema.add(AtlasSource.GEOHASH_STR, StringType, true)
    schema = schema.add(AtlasSource.TIMESTAMP_STR, LongType, true)
    schema
  }

  private var filters: Array[Filter] = Array()

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
    // parse out filters applicable to the atlas file system
    val (atlasFilters, rest) = filters.partition(isAtlasFilter(_))
    this.filters = atlasFilters

    rest
  }

  override def pushedFilters: Array[Filter] = {
    this.filters
  }

  private def isAtlasFilter(filter: Filter): Boolean = {
    filter match {
        case EqualTo(AtlasSource.GEOHASH_STR, _) => true
        case IsNotNull(AtlasSource.GEOHASH_STR) => true
        // TODO - include temporal filters (>, >=, ... etc)
        //case EqualTo(AtlasSource.TIMESTAMP_STR, _) => true
        //case IsNotNull(AtlasSource.TIMESTAMP_STR) => true
        case _ => false
    }
  }
}
