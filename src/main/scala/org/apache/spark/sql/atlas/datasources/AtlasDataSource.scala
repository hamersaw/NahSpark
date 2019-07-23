package org.apache.spark.sql.atlas.datasources

import com.bushpath.anamnesis.ipc.rpc.RpcClient

import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.sources.{DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}


import com.bushpath.atlas.spark.sql.util.Parser

import scala.collection.mutable.ListBuffer

class AtlasDataSource extends DataSourceRegister with RelationProvider {
  override def shortName() = "atlas"

  override def createRelation(sqlContext: SQLContext,
      parameters: Map[String, String]) = {
    // initialize FileIndex
    val fileIndex = new InMemoryFileIndex(sqlContext.sparkSession,
      Seq(new Path(parameters("path"))), Map(), None)

    // retrieve FileStatus and storage policy of each file
    var storagePolicyId: Option[Int] = None
    var storagePolicy = ""
    val files = fileIndex.inputFiles.map { url =>
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

    val (fileFormat, options) = fileFormatString match {
      case "CsvPoint" => (new CSVFileFormat(),
        Map("inferSchema" -> "true")); 
      case "Wkt" => (new CSVFileFormat(),
        Map("inferSchema" -> "true", "delimiter" -> "\t"));
    }

    // infer schema and substitute geometry
    val inferredSchema = fileFormat.inferSchema(
      sqlContext.sparkSession, options, files)
    val dataSchema = inferredSchema.orNull

    // initialize AtlasRelation
    new AtlasRelation(sqlContext, fileIndex.inputFiles,
      fileIndex.sizeInBytes, dataSchema, fileFormatString)
  }
}
