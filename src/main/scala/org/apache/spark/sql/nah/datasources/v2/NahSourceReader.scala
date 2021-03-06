package org.apache.spark.sql.nah.datasources.v2

import io.blackpine.geohash.Geohash
import io.blackpine.hdfs_comm.ipc.rpc.RpcClient
import io.blackpine.nah.spark.sql.util.Converter
import io.blackpine.nahfs.protocol.proto.NahFSProtos

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

import java.util.{ArrayList, List}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

class NahSourceReader(blockMap: Map[Long, Long],
    blockLocations: Map[Long, ListBuffer[String]],
    datanodeMap: Map[String, HdfsProtos.DatanodeIDProto], 
    dataSchema: StructType, fileFormat: String,
    formatFields: Map[String, String], maxPartitionBytes: Long,
    lookAheadBytes: Long) extends DataSourceReader
    with SupportsPushDownFilters with SupportsPushDownRequiredColumns {
  private final val FIRST_BIT = -9223372036854775808l;
  private final val INDEXED_MASK = -4294967296l;

  private var requiredSchema = {
    val schema = StructType(dataSchema)
    schema
  }

  private var filters: Array[Filter] = Array()

  override def readSchema: StructType = requiredSchema

  override def planInputPartitions
      : List[InputPartition[InternalRow]] = {
    //val filterCompileStart = System.currentTimeMillis // TODO - remove

    // compile nah filter query
    val latitude = getLatitudeFeature
    val longitude = getLongitudeFeature
    var minLat= -java.lang.Double.MAX_VALUE
    var maxLat= java.lang.Double.MAX_VALUE
    var minLong= -java.lang.Double.MAX_VALUE
    var maxLong= java.lang.Double.MAX_VALUE

    // process filters
    for (filter <- filters) {
      filter match {
        case GreaterThan(a, b) => {
          if (a == latitude) {
            minLat= scala.math.max(minLat, Converter.toDouble(b))
          } else if (a == longitude) {
            minLong= scala.math.max(minLong, Converter.toDouble(b))
          }
        }
        case GreaterThanOrEqual(a, b) => {
          if (a == latitude) {
            minLat= scala.math.max(minLat, Converter.toDouble(b))
          } else if (a == longitude) {
            minLong= scala.math.max(minLong, Converter.toDouble(b))
          }
        }
        case LessThan(a, b) => {
          if (a == latitude) {
            maxLat= scala.math.min(maxLat, Converter.toDouble(b))
          } else if (a == longitude) {
            maxLong= scala.math.min(maxLong, Converter.toDouble(b))
          }
        }
        case LessThanOrEqual(a, b) => {
          if (a == latitude) {
            maxLat= scala.math.min(maxLat, Converter.toDouble(b))
          } else if (a == longitude) {
            maxLong= scala.math.min(maxLong, Converter.toDouble(b))
          }
        }
        case _ => println("TODO - support filter: " + filter)
      }
    }

    //println("(" + minLat + " " + maxLat
    //  + " " + minLong + " " + maxLong + ")")

    // calculate bounding geohash
    val geohashBound1 = Geohash.encode16(minLat, minLong, 6)
    val geohashBound2 = Geohash.encode16(minLat, maxLong, 6)
    val geohashBound3 = Geohash.encode16(maxLat, minLong, 6)
    val geohashBound4 = Geohash.encode16(maxLat, maxLong, 6)

    //println(geohashBound1 + " : " + geohashBound2
    //  + " : " + geohashBound3 + " : " + geohashBound3)

    var count = 0;
    breakable {
      while (true) {
        if (geohashBound1(count) == geohashBound2(count)
            && geohashBound1(count) == geohashBound3(count)
            && geohashBound1(count) == geohashBound4(count)) {
          count += 1
        } else {
          break
        }
      }
    }

    val geohashBound = geohashBound1.substring(0, count)
    //println("BOUNDING GEOHASH: '" + geohashBound + "'")
    //val nahQuery = nahQueryExpressions.mkString("&")
 
    var nahQuery = "";
    if (!geohashBound.isEmpty) {
      nahQuery += "g=" + geohashBound
    }

    // TODO - remove
    //val filterCompileDuration = System.currentTimeMillis - filterCompileStart
    //println("filterCompileDuration: " + filterCompileDuration)

    //val filterExecStart = System.currentTimeMillis // TODO - remove

    var processBlockMap = Map[Long, Long]()
    if (nahQuery.isEmpty) {
      processBlockMap = blockMap
    } else {
      // split blocks along namenode
      var blockSources = Map[String, ListBuffer[Long]]()
      for ((blockId, locations) <- blockLocations) {
        val namenode = locations.head.split("-").head 
        blockSources.get(namenode) match {
          case Some(blockIds) => blockIds += blockId
          case None => {
            val blockIds = new ListBuffer[Long]()
            blockIds += blockId
            blockSources += (namenode -> blockIds)
          }
        }
      }

      // perform filter on each block source
      for ((namenode, blockIds) <- blockSources) {
        val namenodeFields = namenode.split(":")
        val (host, port) = (namenodeFields(0), namenodeFields(1).toInt)

        // send BlockFilterRequestProto request
        val bfRpcClient = new RpcClient(host, port, "ATLAS-SPARK",
          "io.blackpine.nahfs.protocol.NahFSProtocol")
        val bfRequestBuilder = NahFSProtos.BlockFilterRequestProto
          .newBuilder()
            .setFilter(nahQuery)

        for (blockId <- blockIds) {
          bfRequestBuilder.addBlockIds(blockId)
        }

        val bfIn = bfRpcClient.send("filterBlocks", bfRequestBuilder.build())
        val bfResponse =
          NahFSProtos.BlockFilterResponseProto.parseDelimitedFrom(bfIn)
        bfIn.close
        bfRpcClient.close

        // process BlockFilterResponseProto
        for (i <- 0 to bfResponse.getBlockIdsCount - 1) {
          processBlockMap += (bfResponse.getBlockIds(i) 
            -> bfResponse.getBlockLengths(i))
        }
      }
    }

    // TODO - remove
    //val filterExecDuration = System.currentTimeMillis - filterExecStart
    //println("filterExecDuration: " + filterExecDuration)

    //val partitionStart = System.currentTimeMillis // TODO - remove

    // compute partitions
    val partitions: List[InputPartition[InternalRow]] = new ArrayList()
    for ((blockId, blockLength) <- processBlockMap) {
      // compute locationBlockId -> if indexed only use first 32 bits
      var locationBlockId = blockId
      if ((blockId & FIRST_BIT) == FIRST_BIT) { // indexed block
        locationBlockId = locationBlockId & INDEXED_MASK
      }

      val locations = blockLocations.get(locationBlockId).orNull
        .map(datanodeMap.get(_).orNull.getIpAddr).toArray
      val ports = blockLocations.get(locationBlockId).orNull
        .map(datanodeMap.get(_).orNull.getXferPort).toArray

      // split blocks into multiple partitions of maxPartitionBytes
      var offset = 0l
      while (offset < blockLength) {
        val length = scala.math.min(maxPartitionBytes,
          blockLength - offset)
        val lookAhead = scala.math.min(lookAheadBytes,
          blockLength - (offset + length))

        // initialize block partition
        partitions += new NahPartition(dataSchema, requiredSchema, blockId,
          offset, length, offset == 0, lookAhead, locations, ports)

        offset += length
      }
    }

    // TODO - remove
    //val partitionDuration = System.currentTimeMillis - partitionStart
    //println("partitionDuration: " + partitionDuration)

    partitions
  }

  override def pruneColumns(requiredSchema: StructType) = {
    this.requiredSchema = requiredSchema
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // parse out filters applicable to the nah file system
    val (validFilters, rest) = filters.partition(isValidFilter(_))
    this.filters = validFilters

    rest
  }

  override def pushedFilters: Array[Filter] = {
    this.filters
  }

  private def getLatitudeFeature(): String = {
    fileFormat match {
      case "CsvPoint" => {
        val index = formatFields("latitude_index").toInt
        dataSchema.fieldNames(index)
      }
      case _ => null
    }
  }

  private def getLongitudeFeature(): String = {
    fileFormat match {
      case "CsvPoint" => {
        val index = formatFields("longitude_index").toInt
        dataSchema.fieldNames(index)
      }
      case _ => null
    }
  }

  private def isValidFilter(filter: Filter): Boolean = {
    val latitude = getLatitudeFeature
    val longitude = getLongitudeFeature
    
    filter match {
      case GreaterThan(x, _) => x == latitude || x == longitude
      case GreaterThanOrEqual(x, _) =>  x == latitude || x == longitude
      case LessThan(x, _) =>  x == latitude || x == longitude
      case LessThanOrEqual(x, _) =>  x == latitude || x == longitude
      case _ => false
    }
  }
}
