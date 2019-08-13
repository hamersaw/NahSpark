package org.apache.spark.sql.atlas.datasources.v2

import com.bushpath.anamnesis.checksum.ChecksumFactory
import com.bushpath.anamnesis.ipc.datatransfer.{BlockInputStream, DataTransferProtocol}
import com.bushpath.anamnesis.ipc.rpc.RpcClient

import com.google.protobuf.ByteString

import org.apache.hadoop.hdfs.protocol.proto.{HdfsProtos, ClientNamenodeProtocolProtos}

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.types.{StringType, StructType}

import com.bushpath.atlas.spark.sql.util.Parser

import java.io.{BufferedInputStream, ByteArrayInputStream, DataInputStream, DataOutputStream}
import java.net.Socket
import java.util.Scanner

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object AtlasSource {
  final val GEOHASH_FIELD = "atlasGeohash"
  final val TIMESTAMP_FIELD = "atlasTimestamp"
}

class AtlasSource extends DataSourceV2 with ReadSupport with DataSourceRegister {
  override def shortName() = "atlas"

  override def createReader(options: DataSourceOptions)
      : DataSourceReader = {
    println("AtlasSource.createReader")

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
            case Some(id) => {
              if (id != hfsProto.getStoragePolicy) {
                println("TODO - storagePolicy differs on input files")
              }
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

    // compile dataSchema
    val dataSchema: StructType =
        options.get("inferSchema").orElse("false") match {
      case "true" => {
        // compile file list
        val files = new ListBuffer[FileStatus]()
        for ((_, f) <- fileMap) {
          files ++= f
        }

        // inferSchema using Spark FileFormat
        fileFormatString match {
          case "CsvPoint" => {
            new CSVFileFormat().inferSchema(SparkSession.active,
              Map("inferSchema" -> "true"), files).orNull
          }
          case "Wkt" => {
            new CSVFileFormat().inferSchema(SparkSession.active,
              Map("inferSchema" -> "true", "delimiter" -> "\t"), files).orNull
          }
        }
      }
      case _ => {
        // read first block
        val (id, files) = fileMap.head
        val file = files.head

        // parse ipAddress, port
        val idFields = id.split(":")
        val (ipAddress, port) = (idFields(0), idFields(1).toInt)

        // send GetFileInfo request
        val gblRpcClient = new RpcClient(ipAddress, port, "ATLAS-SPARK",
          "org.apache.hadoop.hdfs.protocol.ClientProtocol")
        val gblRequest = ClientNamenodeProtocolProtos
          .GetBlockLocationsRequestProto.newBuilder()
            .setSrc(file.getPath.toString)
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
        val lbProto = lbsProto.getBlocks(0)
        
        val blockId = lbProto.getB.getBlockId
        val blockLength = lbProto.getB.getNumBytes

        // parse locations
        val blockData = new Array[Byte](blockLength.toInt)
        breakable { for (diProto <- lbProto.getLocsList) {
          val didProto = diProto.getId

          val socket = new Socket(didProto.getIpAddr, didProto.getXferPort)
          val dataOut = new DataOutputStream(socket.getOutputStream)
          val dataIn = new DataInputStream(socket.getInputStream)

          // send read block op and recv response
          DataTransferProtocol.sendReadOp(dataOut, "default-pool",
            blockId, 0, "AtlasPartitionReader", 0, blockLength)
          val blockOpResponse = DataTransferProtocol
            .recvBlockOpResponse(dataIn)

          // recv block data
          val blockIn = new BlockInputStream(dataIn, dataOut,
            ChecksumFactory.buildDefaultChecksum)

          var offset = 0
          var bytesRead = 0
          while (offset < blockData.length) {
            bytesRead = blockIn.read(blockData,
              offset, blockData.length - offset)
            offset += bytesRead
          }

          blockIn.close
          dataIn.close
          dataOut.close
          socket.close

          //  TODO - check for success
          break
        } }

        // parse field count from block
        var delimiterCount = 0
        val inputStream = new ByteArrayInputStream(blockData)
        val bufferedInputStream = new BufferedInputStream(inputStream)
        val scanner = new Scanner(bufferedInputStream, "UTF-8")

        breakable { while (scanner.hasNextLine) {
          val line = scanner.nextLine
          val count = line.count(_ == ',')

          if (delimiterCount == 0) {
            delimiterCount = count
          } else if (delimiterCount == count) {
            break
          } else {
            // TODO - throw error
          }
        }}

        scanner.close()
        bufferedInputStream.close()
        inputStream.close()

        // compile StructType
        var dataSchema = new StructType()
        for (i <- 0 to delimiterCount) {
          dataSchema = dataSchema.add("_c" + i, StringType, true)
        }

        dataSchema
      }
    }

    // return new AtlasSourceReader
    new AtlasSourceReader(fileMap, dataSchema)
  }
}
