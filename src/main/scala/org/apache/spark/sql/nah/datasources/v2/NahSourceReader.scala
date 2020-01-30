package org.apache.spark.sql.nah.datasources.v2

import com.bushpath.geohash.Geohash
import com.bushpath.hdfs_comm.ipc.rpc.RpcClient
import com.bushpath.nah.spark.sql.util.Converter

import org.apache.hadoop.hdfs.protocol.proto.{ClientNamenodeProtocolProtos, HdfsProtos}
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{EqualTo, GreaterThan, GreaterThanOrEqual, IsNotNull, LessThan, LessThanOrEqual}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.{LongType, StructType}

import java.lang.Thread
import java.util.{ArrayList, HashSet, List}
import java.util.concurrent.ArrayBlockingQueue

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

class NahSourceReader(fileMap: Map[String, Seq[FileStatus]],
    dataSchema: StructType, fileFormat: String,
    formatFields: Map[String, String]) extends DataSourceReader
    with SupportsPushDownFilters with SupportsPushDownRequiredColumns {
  private var requiredSchema = {
    var schema = StructType(dataSchema)
    schema
  }

  private var filters: Array[Filter] = Array()

  override def readSchema: StructType = requiredSchema

  override def planInputPartitions
      : List[InputPartition[InternalRow]] = {
    // compile nah filter query
    var nahQueryExpressions = new ListBuffer[String]()

    val latitude = getLatitudeFeature
    val longitude = getLongitudeFeature
    var minLat= -java.lang.Double.MAX_VALUE
    var maxLat= java.lang.Double.MAX_VALUE
    var minLong= -java.lang.Double.MAX_VALUE
    var maxLong= java.lang.Double.MAX_VALUE

    // process filters
    for (filter <- filters) {
      //println("NahSourceReader process filter: " + filter)

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

    //println("(" + minLat+ " " 
    //  + maxLat+ " " + minLong+ " " + maxLong+ ")")

    // calculate bounding geohash
    val geohashBound1 = Geohash.encode16(minLat, minLong, 6)
    val geohashBound2 = Geohash.encode16(minLat, maxLong, 6)
    val geohashBound3 = Geohash.encode16(maxLat, minLong, 6)
    val geohashBound4 = Geohash.encode16(maxLat, maxLong, 6)

    println(geohashBound1 + " : " + geohashBound2
      + " : " + geohashBound3 + " : " + geohashBound3)

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

    // submit requests within threads
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
