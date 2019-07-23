package org.apache.spark.sql.atlas.datasources

import com.bushpath.atlas.spark.LineParser

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.csv.{CSVOptions, UnivocityParser}
import org.apache.spark.sql.types.{StructField, StructType}

import java.io.InputStream

object AtlasCsvParser {
  def parseStream(inputStream: InputStream,
      dataSchema: StructType, requiredSchema: StructType,
      options: CSVOptions): Iterator[InternalRow] = {
    val parser = new UnivocityParser(dataSchema,
      requiredSchema, options)
    val tokenizer = new LineParser(inputStream)

    this.iterator(tokenizer)(parser.parse)
  }

  private def iterator[T](tokenizer: LineParser)
      (convert: String => T) = new Iterator[T] {
    private var nextRecord = tokenizer.nextRecord()

    override def hasNext: Boolean = nextRecord != null

    override def next(): T = {
      if (!hasNext) {
        throw new NoSuchElementException("End of stream")
      }

      //println("PROCESSING: '" + nextRecord + "'") // TODO - remove
      val currentRecord = convert(nextRecord)
      //println("RESULT: '" + currentRecord + "'") // TODO - remove
      nextRecord = tokenizer.nextRecord()
      currentRecord
    }
  }
}
