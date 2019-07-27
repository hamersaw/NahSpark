package org.apache.spark.sql.atlas.datasources.v2

import com.bushpath.anamnesis.ipc.rpc.RpcClient

import org.apache.hadoop.hdfs.protocol.proto.{ClientNamenodeProtocolProtos, HdfsProtos}

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.StructType;

import com.bushpath.atlas.spark.sql.util.Parser

import java.util.{ArrayList, HashSet, List};

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class AtlasSourceReader(inputFiles: Array[String],
    schema: StructType) extends DataSourceReader {
  override def readSchema: StructType = schema

  override def planInputPartitions
      : List[InputPartition[InternalRow]] = {
    // TODO - change hosts from ipAddr:port to datanodeUuid
    var blocks: Map[Long, HdfsProtos.LocatedBlockProto] = Map()
    var hosts: Map[String, HashSet[Long]] = Map()

    for (inputFile <- inputFiles) {
      // parse url path
      val (ipAddress, port, path) = Parser.parseHdfsUrl(inputFile)

      // send GetFileInfo request
      val gblRpcClient = new RpcClient(ipAddress, port, "ATLAS-SPARK",
        "org.apache.hadoop.hdfs.protocol.ClientProtocol")
      val gblRequest = ClientNamenodeProtocolProtos
        .GetBlockLocationsRequestProto.newBuilder()
          .setSrc(path)
          .setOffset(0)
          .setLength(2147483647)
          .build

      val gblIn = gblRpcClient.send("getBlockLocations", gblRequest)
      val gblResponse = ClientNamenodeProtocolProtos
        .GetBlockLocationsResponseProto.parseDelimitedFrom(gblIn)

      gblIn.close
      gblRpcClient.close

      // process block locations
      val lbProto = gblResponse.getLocations
      for (lbProto <- lbProto.getBlocksList) {
        // parse block id
        val blockId = lbProto.getB.getBlockId
        blocks += (blockId -> lbProto)

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

        partitions += new AtlasPartition(schema, partitionBlocks)
      }
    }

    partitions
  }
}
