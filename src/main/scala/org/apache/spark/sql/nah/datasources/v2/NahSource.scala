package org.apache.spark.sql.nah.datasources.v2

import io.blackpine.hdfs_comm.ipc.rpc.RpcClient

import com.google.protobuf.ByteString

import org.apache.hadoop.hdfs.protocol.proto.{HdfsProtos, ClientNamenodeProtocolProtos}

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

object NahCache {
  var map = Map[String, (Map[Long, Long],
      Map[Long, ListBuffer[String]],
      Map[String, HdfsProtos.DatanodeIDProto], StructType, 
      String, Map[String, String])]()

  def put(paths: String, blockMap: Map[Long, Long],
      blockLocations: Map[Long, ListBuffer[String]],
      datanodeMap: Map[String, HdfsProtos.DatanodeIDProto], 
      dataSchema: StructType, fileFormat: String,
      formatFields: Map[String, String]) = {
    map += (paths -> (blockMap, blockLocations, 
      datanodeMap, dataSchema, fileFormat, formatFields))
  }

  def contains(paths : String): Boolean = {
    map.contains(paths)
  }

  def get(paths: String): (Map[Long, Long],
      Map[Long, ListBuffer[String]],
      Map[String, HdfsProtos.DatanodeIDProto], StructType, 
      String, Map[String, String]) = {
    map(paths)
  }
}

class NahSource extends DataSourceV2 with ReadSupport with DataSourceRegister {
  override def shortName() = "nah"

  override def createReader(options: DataSourceOptions)
      : DataSourceReader = {
    // parse source options
    val maxInferSchemaBytes =
      options.get("maxInferSchemaBytes").orElse("4194304").toLong
    val maxPartitionBytes =
      options.get("maxPartitionBytes").orElse("33554432").toLong
    val lookAheadBytes =
      options.get("lookAheadBytes").orElse("2048").toInt

    // if paths are cached -> return NahSourceReader with cached values
    val paths = options.paths.mkString("&")
    if (NahCache.contains(paths)) {
      val (blockMap, blockLocations, datanodeMap,
        dataSchema, fileFormat, formatFields) = NahCache.get(paths)

      return new NahSourceReader(blockMap, blockLocations,
        datanodeMap, dataSchema, fileFormat, formatFields,
        maxPartitionBytes, lookAheadBytes)
    }

    // discover fileStatus for paths
    var storagePolicyId: Option[Int] = None
    var storagePolicy = ""

    var blockMap = Map[Long, Long]()
    var blockLocations = Map[Long, ListBuffer[String]]()
    var datanodeMap = Map[String, HdfsProtos.DatanodeIDProto]()

    //val filesStart = System.currentTimeMillis // TODO - remove

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
                  }
                }

                blockMap += (blockId -> blockLength)
                blockLocations += (blockId -> locations)
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

    // TODO - remove
    //val filesDuration = System.currentTimeMillis - filesStart
    //println("filesDuration: " + filesDuration)

    //val schemaStart = System.currentTimeMillis // TODO - remove

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
      val readLength = scala.math.min(blockLength, maxInferSchemaBytes)
      val blockData = new Array[Byte](readLength.toInt)
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
        dataOut.writeLong(blockData.length);

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

    // TODO - remove
    //val schemaDuration = System.currentTimeMillis - schemaStart
    //println("schemaDuration: " + schemaDuration)

    // add information to cache
    NahCache.put(paths, blockMap, blockLocations, datanodeMap,
      dataSchema, fileFormat, formatFields)

    // return new NahSourceReader
    new NahSourceReader(blockMap, blockLocations, datanodeMap, dataSchema, 
      fileFormat, formatFields, maxPartitionBytes, lookAheadBytes)
  }
}
