package org.apache.spark.sql.nah.datasources.v2

import io.blackpine.hdfs_comm.ipc.rpc.RpcClient

import com.google.protobuf.ByteString

import org.apache.hadoop.hdfs.protocol.proto.{HdfsProtos, ClientNamenodeProtocolProtos}

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.types.{DoubleType, StructType}

import io.blackpine.nah.spark.sql.util.Parser

import java.io.{BufferedInputStream, ByteArrayInputStream, DataInputStream, DataOutputStream}
import java.net.Socket
import java.util.Scanner
import java.util.regex.Pattern;

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

class NahSource extends DataSourceV2 with ReadSupport with DataSourceRegister {
  override def shortName() = "nah"

  override def createReader(options: DataSourceOptions)
      : DataSourceReader = {
    // discover fileStatus for paths
    var storagePolicyId: Option[Int] = None
    var storagePolicy = ""
    //var fileMap = Map[String, ListBuffer[FileStatus]]() // TODO - remove

    var blockMap = Map[Long, Long]()
    var blockLocations = Map[Long, ListBuffer[String]]()
    var datanodeMap = Map[String, HdfsProtos.DatanodeIDProto]()

    val filesStart = System.currentTimeMillis // TODO - remove

    for (url <- options.paths) {
      // parse url path
      val (host, port, path) = Parser.parseHdfsUrl(url)
      val sourceId = host + ":" + port

      var remainingEntries = -1
      var startAfterEntry = ByteString.EMPTY

      while (remainingEntries != 0) {
        // send GetFileInfo request
        val glRpcClient = new RpcClient(host, port, "ATLAS-SPARK",
          "org.apache.hadoop.hdfs.protocol.ClientProtocol")
        val glRequest = ClientNamenodeProtocolProtos
          .GetListingRequestProto.newBuilder()
            .setSrc(path)
            .setStartAfter(startAfterEntry)
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
          storagePolicyId match {
            case Some(id) => {
              if (id != hfsProto.getStoragePolicy) {
                println("TODO - storagePolicy differs on input files")
              }
            }
            case None => {
              storagePolicyId = Some(hfsProto.getStoragePolicy)
              val gspRpcClient =
                new RpcClient(host, port, "NahSpark",
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
   
          // process file blocks
          hfsProto.getFileType match {
            case HdfsProtos.HdfsFileStatusProto.FileType.IS_FILE => {
              // iterate over file blocks
              for (lbProto <- hfsProto.getLocations.getBlocksList) {
                val blockId = lbProto.getB.getBlockId
                val blockLength = lbProto.getB.getNumBytes

                // iterate over block locations
                val locations = new ListBuffer[String]()
                for (diProto <- lbProto.getLocsList) {
                  val didProto = diProto.getId
                  val locationId = sourceId + "-" + didProto.getDatanodeUuid
                  
                  // add datanodeUuid to this blocks locations
                  locations += locationId

                  // populate datanodeMap with datanodeIdProto
                  if (!datanodeMap.contains(locationId)) {
                    datanodeMap += (locationId -> didProto)
                    //println("added locationId '" + locationId + "'")
                  }
                }

                blockMap += (blockId -> blockLength)
                blockLocations += (blockId -> locations)
                //println("added block '" + blockId + "' locations:" + locations) 
              }

              // TODO - remove
              /*// initialize file
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
              }*/
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

    // TODO - remove
    val filesDuration = System.currentTimeMillis - filesStart
    println("filesDuration: " + filesDuration)

    val schemaStart = System.currentTimeMillis // TODO - remove

    // parse storagePolicy
    val pattern = Pattern.compile("(\\w+)\\((\\w+:\\w+)?(,\\s*\\w+:\\w+)*\\)");
    val fieldsPattern = Pattern.compile("(\\w+):(\\w+)");

    // check for match
    val matcher = pattern.matcher(storagePolicy);
    if (!matcher.matches) {
      println("pattern doesn't match"); // TODO - log? exit?
    }

    // retrieve file format
    matcher.find(0);
    val fileFormat = matcher.group(1);

    // retrieve fields
    var formatFields = Map[String, String]()
    val fieldMatcher = fieldsPattern.matcher(storagePolicy);
    while (fieldMatcher.find) {
      formatFields += (fieldMatcher.group(1) -> fieldMatcher.group(2))
    }

    // compile dataSchema
    val dataSchema: StructType = {
      val (blockId, blockLength) = blockMap.head
      //println("compiling data schema from block '" + blockId + "'")

      // get block data
      val blockData = new Array[Byte](blockLength.toInt)
      val blockLocationIds: Seq[String] =
        blockLocations.get(blockId).orNull

      breakable { for (locationId <- blockLocationIds) {

        val didProto = datanodeMap.get(locationId).orNull
        //println("querying for block locationId '" + locationId + "' " + didProto)

        val socket = new Socket(didProto.getIpAddr, didProto.getXferPort)
        val dataOut = new DataOutputStream(socket.getOutputStream)
        val dataIn = new DataInputStream(socket.getInputStream)

        dataOut.writeShort(28); // protocol version
        dataOut.write(83); // op - ReadBlockDirect
        dataOut.write(0); // protobuf length
        dataOut.writeLong(blockId);
        dataOut.writeLong(0);
        dataOut.writeLong(blockLength);

        var offset = 0;
        var bytesRead = 0;
        while (offset < blockData.length) {
          bytesRead = dataIn.read(blockData, offset,
            blockData.length - offset)
          offset += bytesRead
        }

        // send success indicator
        dataOut.writeByte(0);

        // close streams
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

        if (delimiterCount < count) {
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
        dataSchema = dataSchema.add("_c" + i, DoubleType, true)
      }

      dataSchema
    }

    /*// compile dataSchema
    val dataSchema: StructType =
        options.get("inferSchema").orElse("false") match {
      case "true" => {
        // compile file list
        val files = new ListBuffer[FileStatus]()
        for ((_, f) <- fileMap) {
          files ++= f
        }

        // inferSchema using Spark FileFormat
        fileFormat match {
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
        // TODO - get first block
        // read first block
        val (id, files) = fileMap.head
        val file = files.head

        // parse host, port
        val idFields = id.split(":")
        val (host, port) = (idFields(0), idFields(1).toInt)

        // send GetFileInfo request
        val gblRpcClient = new RpcClient(host, port, "ATLAS-SPARK",
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

          dataOut.writeShort(28); // protocol version
          dataOut.write(83); // op - ReadBlockDirect
          dataOut.write(0); // protobuf length
          dataOut.writeLong(blockId);
          dataOut.writeLong(0);
          dataOut.writeLong(blockLength);

          var offset = 0;
          var bytesRead = 0;
          while (offset < blockData.length) {
            bytesRead = dataIn.read(blockData, offset,
              blockData.length - offset)
            offset += bytesRead
          }

          // send success indicator
          dataOut.writeByte(0);

          // close streams
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
          dataSchema = dataSchema.add("_c" + i, DoubleType, true)
        }

        dataSchema
      }
    }*/

    // TODO - remove
    val schemaDuration = System.currentTimeMillis - schemaStart
    println("schemaDuration: " + schemaDuration)

    // return new NahSourceReader
    val queryThreads = options.get("queryThreads").orElse("16").toInt
    val maxPartitionBytes =
      options.get("maxPartitionBytes").orElse("33554432").toLong
    val lookAheadBytes =
      options.get("lookAheadBytes").orElse("2048").toInt

    //new NahSourceReader(fileMap, dataSchema, fileFormat,
    //  formatFields, queryThreads, maxPartitionBytes, lookAheadBytes)
    new NahSourceReader(blockMap, blockLocations, datanodeMap, 
      dataSchema, fileFormat, formatFields, queryThreads,
      maxPartitionBytes, lookAheadBytes)
  }
}
