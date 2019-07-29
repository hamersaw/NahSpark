package org.apache.spark.sql.atlas.datasources.v2

import com.bushpath.anamnesis.ipc.rpc.RpcClient

import com.google.protobuf.ByteString

import org.apache.hadoop.hdfs.protocol.proto.{HdfsProtos, ClientNamenodeProtocolProtos}

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.sources.v2.reader.DataSourceReader

import com.bushpath.atlas.spark.sql.util.Parser

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object AtlasSource {
  final val GEOHASH_STR = "atlasGeohash"
  final val TIMESTAMP_STR = "atlasTimestamp"
}

class AtlasSource extends DataSourceV2 with ReadSupport with DataSourceRegister {
  override def shortName() = "atlas"

  /*override def createReader(options: DataSourceOptions)
      : DataSourceReader = {
    // use InMemoryFileIndex to discover paths
    val spark = SparkSession.active
    val paths = options.paths.map(x => new Path(x))

    val fileIndex = new InMemoryFileIndex(spark, paths, Map(), None)
    val inputFiles = fileIndex.inputFiles

    // retrieve FileStatus and storage policy of each file
    var storagePolicyId: Option[Int] = None
    var storagePolicy = ""
    val files = inputFiles.map { url =>
      // parse url path
      val (ipAddress, port, path) = Parser.parseHdfsUrl(url)

      // send GetFileInfo request
      val gfiRpcClient = new RpcClient(ipAddress, port, "ATLAS-SPARK",
        "org.apache.hadoop.hdfs.protocol.ClientProtocol")
      val gfiRequest = ClientNamenodeProtocolProtos
        .GetFileInfoRequestProto.newBuilder().setSrc(path).build

      val gfiIn = gfiRpcClient.send("getFileInfo", gfiRequest)
      val gfiResponse = ClientNamenodeProtocolProtos
        .GetFileInfoResponseProto.parseDelimitedFrom(gfiIn)
      val fileStatusProto = gfiResponse.getFs
      gfiIn.close
      gfiRpcClient.close

      // send GetStoragePolicy request
      if (storagePolicyId == None) {
        val gspRpcClient = new RpcClient(ipAddress, port, "ATLAS-SPARK",
          "org.apache.hadoop.hdfs.protocol.ClientProtocol")
        val gspRequest = ClientNamenodeProtocolProtos
          .GetStoragePolicyRequestProto.newBuilder().setPath(path).build

        val gspIn = gspRpcClient.send("getStoragePolicy", gspRequest)
        val gspResponse = ClientNamenodeProtocolProtos
          .GetStoragePolicyResponseProto.parseDelimitedFrom(gspIn)
        val bspProto = gspResponse.getStoragePolicy

        gspIn.close
        gspRpcClient.close

        storagePolicyId = Some(fileStatusProto.getStoragePolicy)
        storagePolicy = bspProto.getName()
      }

      // initialize FileStatus
      new FileStatus(fileStatusProto.getLength, false, 
        fileStatusProto.getBlockReplication, 
        fileStatusProto.getBlocksize, 
        fileStatusProto.getModificationTime, new Path(path))
    }

    // initialize file format and options
    val endIndex = storagePolicy.indexOf("(")
    val fileFormatString = storagePolicy.substring(0, endIndex)

    val (fileFormat, parameters) = fileFormatString match {
      case "CsvPoint" => (new CSVFileFormat(),
        Map("inferSchema" -> "true")); 
      case "Wkt" => (new CSVFileFormat(),
        Map("inferSchema" -> "true", "delimiter" -> "\t"));
    }

    // infer schema
    val schema = fileFormat.inferSchema(spark, parameters, files).orNull

    // return new AtlasSourceReader
    new AtlasSourceReader(inputFiles, schema)
  }*/

  override def createReader(options: DataSourceOptions)
      : DataSourceReader = {
    // discover fileStatus for paths
    var storagePolicyId: Option[Int] = None
    var files = new ListBuffer[FileStatus]()
    var blocks: Map[Long, HdfsProtos.LocatedBlockProto] = Map()

    for (url <- options.paths) {
      // parse url path
      val (ipAddress, port, path) = Parser.parseHdfsUrl(url)

      // send GetFileInfo request
      val glRpcClient = new RpcClient(ipAddress, port, "ATLAS-SPARK",
        "org.apache.hadoop.hdfs.protocol.ClientProtocol")
      val glRequest = ClientNamenodeProtocolProtos
        .GetListingRequestProto.newBuilder()
          .setSrc(path)
          .setStartAfter(ByteString.EMPTY)
          .setNeedLocation(true).build

      val glIn = glRpcClient.send("getListing", glRequest)
      val glResponse = ClientNamenodeProtocolProtos
        .GetListingResponseProto.parseDelimitedFrom(glIn)
      glIn.close
      glRpcClient.close

      // process directory listing
      val dlProto = glResponse.getDirList
      for (hfsProto <- dlProto.getPartialListingList) {
        // set storagePolicyId
        if (storagePolicyId == None) {
          storagePolicyId = Some(hfsProto.getStoragePolicy)
        } // TODO - check if storage policy is the same
 
        // process block locations
        val lbProto = hfsProto.getLocations
        for (lbProto <- lbProto.getBlocksList) {
          // parse block id
          val blockId = lbProto.getB.getBlockId
          blocks += (blockId -> lbProto)
        }

        // initialize FileStatus
        hfsProto.getFileType match {
          case HdfsProtos.HdfsFileStatusProto.FileType.IS_FILE => {
            files += new FileStatus(hfsProto.getLength, false, 
              hfsProto.getBlockReplication, 
              hfsProto.getBlocksize, 
              hfsProto.getModificationTime, new Path(path))
          };
          case _ => {
          };
        }
      }

      // TODO - analyze remaining entries
      if (dlProto.getRemainingEntries != 0) {
        println("TODO - process " + dlProto.getRemainingEntries
          + " remaining entries from path '" + url + "'")
      }
    }

    /*// TODO - get storage policy
    val gspRpcClient = new RpcClient(ipAddress, port, "ATLAS-SPARK",
      "org.apache.hadoop.hdfs.protocol.ClientProtocol")
    val gspRequest = ClientNamenodeProtocolProtos
      .GetStoragePolicyRequestProto.newBuilder().setPath(path).build

    val gspIn = gspRpcClient.send("getStoragePolicy", gspRequest)
    val gspResponse = ClientNamenodeProtocolProtos
      .GetStoragePolicyResponseProto.parseDelimitedFrom(gspIn)
    val bspProto = gspResponse.getStoragePolicy

    gspIn.close
    gspRpcClient.close

    storagePolicy = bspProto.getName()*/
    val storagePolicy = "CsvPoint(a=0,b=1,c=2)"

    // initialize file format and options
    val endIndex = storagePolicy.indexOf("(")
    val fileFormatString = storagePolicy.substring(0, endIndex)

    val (fileFormat, parameters) = fileFormatString match {
      case "CsvPoint" => (new CSVFileFormat(),
        Map("inferSchema" -> "true")); 
      case "Wkt" => (new CSVFileFormat(),
        Map("inferSchema" -> "true", "delimiter" -> "\t"));
    }

    // infer schema
    val spark = SparkSession.active
    val schema = fileFormat.inferSchema(spark, parameters, files).orNull

    // return new AtlasSourceReader
    new AtlasSourceReader(blocks, schema)
  }
}
