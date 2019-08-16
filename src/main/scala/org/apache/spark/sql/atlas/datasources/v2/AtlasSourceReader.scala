package org.apache.spark.sql.atlas.datasources.v2

import com.bushpath.anamnesis.ipc.rpc.RpcClient

import org.apache.hadoop.hdfs.protocol.proto.{ClientNamenodeProtocolProtos, HdfsProtos}
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{EqualTo, GreaterThan, GreaterThanOrEqual, IsNotNull, LessThan, LessThanOrEqual}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.{LongType, StringType, StructType}

import java.lang.Thread
import java.util.{ArrayList, HashSet, List}
import java.util.concurrent.ArrayBlockingQueue

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

class AtlasSourceReader(fileMap: Map[String, Seq[FileStatus]],
    dataSchema: StructType) extends DataSourceReader
    with SupportsPushDownFilters with SupportsPushDownRequiredColumns {
  private var requiredSchema = {
    var schema = StructType(dataSchema)
    schema = schema.add(AtlasSource.GEOHASH_FIELD, StringType, true)
    schema = schema.add(AtlasSource.TIMESTAMP_FIELD, LongType, true)
    schema
  }

  private var filters: Array[Filter] = Array()

  override def readSchema: StructType = requiredSchema

  override def planInputPartitions
      : List[InputPartition[InternalRow]] = {
    // compile atlas filter query
    var atlasQueryExpressions = new ListBuffer[String]()
    for (filter <- filters) {
      filter match {
        case EqualTo(AtlasSource.GEOHASH_FIELD, const) =>
          atlasQueryExpressions += ("g=" + const)
        case GreaterThan(AtlasSource.TIMESTAMP_FIELD, const) =>
          atlasQueryExpressions += ("t>" + const)
        case GreaterThanOrEqual(AtlasSource.TIMESTAMP_FIELD, const) =>
          atlasQueryExpressions += ("t>=" + const)
        case IsNotNull(AtlasSource.GEOHASH_FIELD) => {}
        case IsNotNull(AtlasSource.TIMESTAMP_FIELD) => {}
        case LessThan(AtlasSource.TIMESTAMP_FIELD, const) =>
          atlasQueryExpressions += ("t<" + const)
        case LessThanOrEqual(AtlasSource.TIMESTAMP_FIELD, const) =>
          atlasQueryExpressions += ("t<=" + const)
        case _ => println("TODO - support atlas filter:" + filter)
      }
    }

    val atlasQuery = atlasQueryExpressions.mkString("&")

    // TODO - submit requests within threads
    

    val threadCount = 4
    val inputQueue = new ArrayBlockingQueue[(String, Int, String, Long)](4096)
    val outputQueue = new ArrayBlockingQueue[Any](128)

    val threads: List[Thread] = new ArrayList()
    for (i <- 0 to threadCount) {
      val thread = new Thread() {
        override def run() {
          breakable { while (true) {
            // retrieve next file
            val (ipAddress, port, filename, length) = inputQueue.take()
            if (filename.isEmpty) {
              break
            }

            // send GetFileInfo request
            val gblRpcClient = new RpcClient(ipAddress, port, "ATLAS-SPARK",
              "org.apache.hadoop.hdfs.protocol.ClientProtocol")
            val gblRequest = ClientNamenodeProtocolProtos
              .GetBlockLocationsRequestProto.newBuilder()
                .setSrc(filename)
                .setOffset(0)
                .setLength(length)
                .build

            val gblIn = gblRpcClient.send("getBlockLocations", gblRequest)
            val gblResponse = ClientNamenodeProtocolProtos
              .GetBlockLocationsResponseProto.parseDelimitedFrom(gblIn)
            gblIn.close
            gblRpcClient.close

            // process block locations
            val lbsProto = gblResponse.getLocations
            outputQueue.put(lbsProto)
          } }

          outputQueue.put("")
        }
      }

      thread.start()
      threads.append(thread)
    }

    // push paths down pipeline
    for ((id, files) <- this.fileMap) {
      // parse ipAddress, port
      val idFields = id.split(":")
      val (ipAddress, port) = (idFields(0), idFields(1).toInt)

      for (file <- files) {
        var path = file.getPath.toString
        if (!atlasQuery.isEmpty) {
          path += ("+" + atlasQuery)
        }

        inputQueue.put((ipAddress, port, path, file.getLen))
      }
    }

    for (i <- 0 to threadCount) {
      inputQueue.put(("", 0, "", 0))
    }

    // retrieve block list
    var hosts: Map[String, HashSet[Long]] = Map()
    var blocks: Map[Long, HdfsProtos.LocatedBlockProto] = Map()

    var poisonCount = 0
    while (poisonCount < threadCount) {
      val result = outputQueue.take()
      if (!result.isInstanceOf[HdfsProtos.LocatedBlocksProto]) {
        poisonCount += 1
      } else {
        val lbsProto = result.asInstanceOf[HdfsProtos.LocatedBlocksProto]

        for (lbProto <- lbsProto.getBlocksList) {
          // parse block id
          val blockId = lbProto.getB.getBlockId
          blocks += (blockId -> lbProto)

          // parse locations
          for (diProto <- lbProto.getLocsList) {
            val didProto = diProto.getId
            val address = didProto.getIpAddr + ":" + didProto.getXferPort

            val set = hosts.get(address) match {
              case Some(set) => set
              case None => {
                val set: HashSet[Long] = new HashSet
                hosts += (address -> set)
                set
              }
            }

            set.add(blockId)
          }
        }
      }
    }

    /*for ((id, files) <- this.fileMap) {
      // parse ipAddress, port
      val idFields = id.split(":")
      val (ipAddress, port) = (idFields(0), idFields(1).toInt)

      for (file <- files) {
        // compile file path with atlasQuery
        var path = file.getPath.toString
        if (!atlasQuery.isEmpty) {
          path += ("+" + atlasQuery)
        }

        // send GetFileInfo request
        val gblRpcClient = new RpcClient(ipAddress, port, "ATLAS-SPARK",
          "org.apache.hadoop.hdfs.protocol.ClientProtocol")
        val gblRequest = ClientNamenodeProtocolProtos
          .GetBlockLocationsRequestProto.newBuilder()
            .setSrc(path)
            .setOffset(0)
            .setLength(file.getLen)
            .build

        val gblIn = gblRpcClient.send("getBlockLocations", gblRequest)
        val gblResponse = ClientNamenodeProtocolProtos
          .GetBlockLocationsResponseProto.parseDelimitedFrom(gblIn)
        gblIn.close
        gblRpcClient.close

        // process block locations
        val lbsProto = gblResponse.getLocations
        for (lbProto <- lbsProto.getBlocksList) {
          // parse block id
          val blockId = lbProto.getB.getBlockId
          blocks += (blockId -> lbProto)

          // parse locations
          for (diProto <- lbProto.getLocsList) {
            val didProto = diProto.getId
            val address = didProto.getIpAddr + ":" + didProto.getXferPort

            val set = hosts.get(address) match {
              case Some(set) => set
              case None => {
                val set: HashSet[Long] = new HashSet
                hosts += (address -> set)
                set
              }
            }

            set.add(blockId)
          }
        }
      }
    }*/

    // compute partitions
    val partitions: List[InputPartition[InternalRow]] = new ArrayList()
    var primaryLocations: Map[String, Int] = Map()

    for ((blockId, lbProto) <- blocks) {
      var locations = new ListBuffer[String]()
      breakable { while (true) {
        // find host containing block with shortest blockId list
        var blockHostOption: Option[String] = None
        var blockHostLength = Integer.MAX_VALUE

        for (host <- hosts.keys) {
          val set = hosts(host)
          if (set.contains(blockId)) {
            if (blockHostOption == None 
              || set.size < blockHostLength
              || (set.size == blockHostLength &&
                primaryLocations.get(blockHostOption.orNull).getOrElse(0) 
                  > primaryLocations.get(host).getOrElse(0))) {
              blockHostOption = Some(host)
              blockHostLength = set.size
            }
          }
        }

        blockHostOption match {
          // if host found -> add to locations
          case Some(blockHost) => {
            locations += blockHost
            hosts.get(blockHost).orNull.remove(blockId)

            if (locations.length == 1) {
              val x = primaryLocations.get(blockHost).getOrElse(0)
              primaryLocations += blockHost -> (x + 1)
            }
          }
          // if host not found -> no hosts left -> break
          case None => break
        }
      } }

      println("partition " + blockId + " " + locations.toList)

      // initialize block partition
      partitions += new AtlasPartition(dataSchema, requiredSchema,
        blockId, lbProto.getB.getNumBytes, locations.toArray)
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
      case EqualTo(AtlasSource.GEOHASH_FIELD, _) => true
      case GreaterThan(AtlasSource.TIMESTAMP_FIELD, _) => true
      case GreaterThanOrEqual(AtlasSource.TIMESTAMP_FIELD, _) => true
      case IsNotNull(AtlasSource.GEOHASH_FIELD) => true
      case IsNotNull(AtlasSource.TIMESTAMP_FIELD) => true
      case LessThan(AtlasSource.TIMESTAMP_FIELD, _) => true
      case LessThanOrEqual(AtlasSource.TIMESTAMP_FIELD, _) => true
      case _ => false
    }
  }
}
