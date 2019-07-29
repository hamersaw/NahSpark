package org.apache.spark.sql.atlas.datasources.v2

import com.bushpath.anamnesis.checksum.ChecksumFactory
import com.bushpath.anamnesis.ipc.datatransfer.{BlockInputStream, DataTransferProtocol}

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos

import org.apache.spark.sql.catalyst.InternalRow;
// TODO - remove UnivocityParser dependencies if using our CsvParser
import org.apache.spark.sql.execution.datasources.csv.{CSVOptions, UnivocityParser}
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType;

import java.io.{BufferedInputStream, ByteArrayInputStream, DataInputStream, DataOutputStream}
import java.net.Socket
import java.util.Scanner

import scala.collection.JavaConversions._
import scala.util.control.Breaks._

class AtlasPartitionReader(dataSchema: StructType,
    requiredSchema: StructType,
    blocks: Seq[HdfsProtos.LocatedBlockProto])
    extends InputPartitionReader[InternalRow] {
  val csvOptions = new CSVOptions(Map(), false, "TODO - time zone")
  //val parser = new UnivocityParser(dataSchema, requiredSchema, csvOptions)
  val parser = new CsvParser(dataSchema, requiredSchema, csvOptions)

  var index = 0

  var inputStream: ByteArrayInputStream = null
  var bufferedInputStream: BufferedInputStream = null
  var scanner = getNextBlock

  private def getNextBlock: Scanner = {
    // close existing streams
    this.close

    // get next block
    val lbProto = blocks(this.index)
    val ebProto = lbProto.getB

    // retrieve block data
    val blockId = ebProto.getBlockId
    val length = ebProto.getNumBytes
    val blockData = new Array[Byte](length.toInt)

    // read blocks from preferred locations first
    breakable { for (diProto <- lbProto.getLocsList) {
      val didProto = diProto.getId

      val socket = new Socket(didProto.getIpAddr, didProto.getXferPort)
      val dataOut = new DataOutputStream(socket.getOutputStream)
      val dataIn = new DataInputStream(socket.getInputStream)

      // send read block op and recv response
      DataTransferProtocol.sendReadOp(dataOut, "default-pool",
        blockId, 0, "AtlasPartitionReader", 0, length)
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

      break // TODO - check for success
    } }

    this.index += 1

    // open new streams
    this.inputStream = new ByteArrayInputStream(blockData)
    this.bufferedInputStream = new BufferedInputStream(this.inputStream)
    new Scanner(this.bufferedInputStream, "UTF-8")
  }

  override def next: Boolean = {
    while (index < blocks.size && !scanner.hasNextLine) {
      this.scanner = this.getNextBlock
    }

    scanner.hasNextLine
  }

  override def get: InternalRow = {
    val line = scanner.nextLine

    parser.parse(line)
  }

  override def close = {
    if (this.scanner != null) {
      this.scanner.close
      this.bufferedInputStream.close
      this.inputStream.close
    }
  }
}
