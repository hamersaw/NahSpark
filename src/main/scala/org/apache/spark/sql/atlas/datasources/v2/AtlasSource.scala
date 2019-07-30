package org.apache.spark.sql.atlas.datasources.v2

import com.bushpath.anamnesis.ipc.rpc.RpcClient

import com.google.protobuf.ByteString

import org.apache.hadoop.hdfs.protocol.proto.{HdfsProtos, ClientNamenodeProtocolProtos}

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.sources.v2.reader.DataSourceReader

import com.bushpath.atlas.spark.sql.util.Parser

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object AtlasSource {
  final val GEOHASH_FIELD = "atlasGeohash"
  final val TIMESTAMP_FIELD = "atlasTimestamp"
}

class AtlasSource extends DataSourceV2 with ReadSupport with DataSourceRegister {
  override def shortName() = "atlas"

  override def createReader(options: DataSourceOptions)
      : DataSourceReader = {
    // discover fileStatus for paths
    var storagePolicyId: Option[Int] = None
    var storagePolicy = ""
    var fileMap = Map[String, ListBuffer[FileStatus]]()

    for (url <- options.paths) {
      // parse url path
      val (ipAddress, port, path) = Parser.parseHdfsUrl(url)

      val id = ipAddress + ":" + port
      var remainingEntries = -1
      var startAfterEntry = ByteString.EMPTY

      while (remainingEntries != 0) {
        // send GetFileInfo request
        val glRpcClient = new RpcClient(ipAddress, port, "ATLAS-SPARK",
          "org.apache.hadoop.hdfs.protocol.ClientProtocol")
        val glRequest = ClientNamenodeProtocolProtos
          .GetListingRequestProto.newBuilder()
            .setSrc(path)
            .setStartAfter(startAfterEntry)
            .setNeedLocation(false).build

        val glIn = glRpcClient.send("getListing", glRequest)
        val glResponse = ClientNamenodeProtocolProtos
          .GetListingResponseProto.parseDelimitedFrom(glIn)
        glIn.close
        glRpcClient.close

        // process directory listing
        val dlProto = glResponse.getDirList
        for (hfsProto <- dlProto.getPartialListingList) {
          // set storagePolicyId
          storagePolicyId match {
            case Some(id) if id != hfsProto.getStoragePolicy => {
              println("TODO - storagePolicy differs on input files")
            }
            case None => {
              storagePolicyId = Some(hfsProto.getStoragePolicy)
              val gspRpcClient = 
                new RpcClient(ipAddress, port, "ATLAS-SPARK",
                "org.apache.hadoop.hdfs.protocol.ClientProtocol")
              val gspRequest = ClientNamenodeProtocolProtos
                .GetStoragePolicyRequestProto.newBuilder()
                  .setPath(path).build

              val gspIn = 
                gspRpcClient.send("getStoragePolicy", gspRequest)
              val gspResponse = ClientNamenodeProtocolProtos
                .GetStoragePolicyResponseProto.parseDelimitedFrom(gspIn)
              val bspProto = gspResponse.getStoragePolicy

              gspIn.close
              gspRpcClient.close

              storagePolicy = bspProto.getName()
            }
          }
   
          // initialize FileStatus
          hfsProto.getFileType match {
            case HdfsProtos.HdfsFileStatusProto.FileType.IS_FILE => {
              // initialize file
              val filePath = new String(hfsProto.getPath.toByteArray)
              val file = new FileStatus(hfsProto.getLength, false, 
                hfsProto.getBlockReplication, 
                hfsProto.getBlocksize, 
                hfsProto.getModificationTime, new Path(filePath))

              // add file to fileMap
              fileMap.get(id) match {
                case Some(files) => files += file
                case None => {
                  val files = new ListBuffer[FileStatus]()
                  files += file
                  fileMap += (id -> files)
                }
              }
            }
            case _ => {}
          }

          // update startAfterEntry instance variable
          startAfterEntry = hfsProto.getPath
        }

        // update remainingEntries instance variable
        remainingEntries = dlProto.getRemainingEntries
      }
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
    val files = new ListBuffer[FileStatus]()
    for ((_, f) <- fileMap) {
      files ++= f
    }

    val inferredSchema = fileFormat.inferSchema(SparkSession.active,
      parameters, files).orNull

    // return new AtlasSourceReader
    new AtlasSourceReader(fileMap, inferredSchema)
  }
}
