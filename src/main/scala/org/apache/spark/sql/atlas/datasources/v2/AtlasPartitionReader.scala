package org.apache.spark.sql.atlas.datasources.v2

import com.bushpath.anamnesis.checksum.ChecksumFactory
import com.bushpath.anamnesis.ipc.datatransfer.{BlockInputStream, DataTransferProtocol}

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.csv.CSVOptions
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType

import java.io.{BufferedInputStream, ByteArrayInputStream, DataInputStream, DataOutputStream}
import java.net.Socket
import java.util.Scanner

import scala.collection.JavaConversions._
import scala.util.control.Breaks._

class AtlasPartitionReader(dataSchema: StructType,
    requiredSchema: StructType, blockId: Long, blockLength: Long,
    locations: Array[String]) extends InputPartitionReader[InternalRow] {
  val csvOptions = new CSVOptions(Map(), false, "TODO - time zone")
  val parser = new CsvParser(dataSchema, requiredSchema, csvOptions)
  val inputStream = {
    val blockData = new Array[Byte](blockLength.toInt)

    // read blocks from preferred locations first
    // TODO - find which node we're on
    breakable { for (location <- locations) {
      //val blockStart = System.currentTimeMillis
      val locationFields = location.split(":")
      val (ipAddress, port) = (locationFields(0), locationFields(1).toInt)

      val socket = new Socket(ipAddress, port)
      val dataOut = new DataOutputStream(socket.getOutputStream)
      val dataIn = new DataInputStream(socket.getInputStream)

      // send read block op and recv response
      DataTransferProtocol.sendReadOp(dataOut, "default-pool",
        blockId, 0, "direct-client", 0, blockLength)
      //DataTransferProtocol.sendReadOp(dataOut, "default-pool",
        //blockId, 0, "AtlasPartitionReader", 0, blockLength)
      val blockOpResponse = DataTransferProtocol
        .recvBlockOpResponse(dataIn)

      // recv block data
      //val blockIn = new BlockInputStream(dataIn, dataOut,
      //  ChecksumFactory.buildDefaultChecksum)

      var offset = 0;
      var bytesRead = 0;
      while (offset < blockData.length) {
        bytesRead = dataIn.read(blockData, offset,
          blockData.length - offset)
        //bytesRead = blockIn.read(blockData, offset,
        //  blockData.length - offset)
        offset += bytesRead
      }

      dataOut.writeByte(0);

      //blockIn.close
      dataIn.close
      dataOut.close
      socket.close

      //val blockDuration = System.currentTimeMillis - blockStart
      //println("AtlasPartitionReader - block - " + blockDuration)

      break // TODO - check for success
    } }

    // open input streams
    new ByteArrayInputStream(blockData)
  }

  val bufferedInputStream = new BufferedInputStream(this.inputStream)
  val scanner = new Scanner(this.bufferedInputStream, "UTF-8")

  override def next: Boolean = {
    this.scanner.hasNextLine
  }

  override def get: InternalRow = {
    val line = this.scanner.nextLine

    parser.parse(line)
  }

  override def close = {
    this.scanner.close
    this.bufferedInputStream.close
    this.inputStream.close
  }
}
