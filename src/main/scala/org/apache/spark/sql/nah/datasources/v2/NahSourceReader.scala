package org.apache.spark.sql.nah.datasources.v2

import com.bushpath.hdfs_comm.ipc.rpc.RpcClient

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

class NahSourceReader(fileMap: Map[String, Seq[FileStatus]],
    dataSchema: StructType) extends DataSourceReader
    with SupportsPushDownFilters with SupportsPushDownRequiredColumns {
  private var requiredSchema = {
    var schema = StructType(dataSchema)
    // TODO - remove
    //schema = schema.add(NahSource.GEOHASH_FIELD, StringType, true)
    //schema = schema.add(NahSource.TIMESTAMP_FIELD, LongType, true)
    schema
  }

  private var filters: Array[Filter] = Array()

  override def readSchema: StructType = requiredSchema

  override def planInputPartitions
      : List[InputPartition[InternalRow]] = {
    // compile nah filter query
    var nahQueryExpressions = new ListBuffer[String]()
    // TODO - process filters
    for (filter <- filters) {
      filter match {
        case _ => println("TODO - support filter: " + filter)
      }
    }

    /*for (filter <- filters) {
      filter match {
        case EqualTo(NahSource.GEOHASH_FIELD, const) =>
          nahQueryExpressions += ("g=" + const)
        case GreaterThan(NahSource.TIMESTAMP_FIELD, const) =>
          nahQueryExpressions += ("t>" + const)
        case GreaterThanOrEqual(NahSource.TIMESTAMP_FIELD, const) =>
          nahQueryExpressions += ("t>=" + const)
        case IsNotNull(NahSource.GEOHASH_FIELD) => {}
        case IsNotNull(NahSource.TIMESTAMP_FIELD) => {}
        case LessThan(NahSource.TIMESTAMP_FIELD, const) =>
          nahQueryExpressions += ("t<" + const)
        case LessThanOrEqual(NahSource.TIMESTAMP_FIELD, const) =>
          nahQueryExpressions += ("t<=" + const)
        case _ => println("TODO - support nah filter:" + filter)
      }
    }*/

    val nahQuery = nahQueryExpressions.mkString("&")

    // TODO - submit requests within threads
    val threadCount = 4
    val inputQueue = new ArrayBlockingQueue[(String, Int, String, Long)](4096)
    val outputQueue = new ArrayBlockingQueue[Any](128)

    val threads: List[Thread] = new ArrayList()
    for (i <- 1 to threadCount) {
      val thread = new Thread() {
        override def run() {
          breakable { while (true) {
            // retrieve next file
            val (ipAddress, port, filename, length) = inputQueue.take()
            if (filename.isEmpty) {
              break
            }

            //println("processing file '" + filename + ":" + length);

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
        if (!nahQuery.isEmpty) {
          path += ("+" + nahQuery)
        }

        inputQueue.put((ipAddress, port, path, file.getLen))
      }
    }

    for (i <- 1 to threadCount) {
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
          //println("added block '" + blockId + "'")

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
        // compile file path with nahQuery
        var path = file.getPath.toString
        if (!nahQuery.isEmpty) {
          path += ("+" + nahQuery)
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

      //println("partition " + blockId + " " + locations.toList)

      // initialize block partition
      partitions += new NahPartition(dataSchema, requiredSchema,
        blockId, lbProto.getB.getNumBytes, locations.toArray)
    } 

    partitions
  }

  override def pruneColumns(requiredSchema: StructType) = {
    this.requiredSchema = requiredSchema
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // TODO - check this?
    for (filter <- filters) {
      println("TESTING TO USE FILTER: " + filter)
    }

    // parse out filters applicable to the nah file system
    val (nahFilters, rest) = filters.partition(isNahFilter(_))
    this.filters = nahFilters

    rest
  }

  override def pushedFilters: Array[Filter] = {
    this.filters
  }

  private def isNahFilter(filter: Filter): Boolean = {
    filter match {
      // TODO - fix this code
      //case EqualTo(NahSource.GEOHASH_FIELD, _) => true
      //case GreaterThan(NahSource.TIMESTAMP_FIELD, _) => true
      //case GreaterThanOrEqual(NahSource.TIMESTAMP_FIELD, _) => true
      //case IsNotNull(NahSource.GEOHASH_FIELD) => true
      //case IsNotNull(NahSource.TIMESTAMP_FIELD) => true
      //case LessThan(NahSource.TIMESTAMP_FIELD, _) => true
      //case LessThanOrEqual(NahSource.TIMESTAMP_FIELD, _) => true
      case _ => true
    }
  }
}
