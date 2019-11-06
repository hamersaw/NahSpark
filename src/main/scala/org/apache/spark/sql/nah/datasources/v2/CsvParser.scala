package org.apache.spark.sql.nah.datasources.v2

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.csv.{CSVOptions, UnivocityParser}
import org.apache.spark.sql.types.StructType;

class CsvParser(dataSchema: StructType,
    requiredSchema: StructType, options: CSVOptions) 
    extends UnivocityParser(new StructType(), options) {
  private val row = new GenericInternalRow(requiredSchema.length)

  private val valueConverters: Array[Array[String] => Any] = {
    requiredSchema.map(f => 
      if (dataSchema.contains(f)) {
        val index = dataSchema.fieldIndex(f.name)
        val converter = makeConverter(f.name,
          f.dataType, f.nullable, options)

        (row: Array[String]) => converter(row(index))
      } else {
        (_: Array[String]) => null
      }).toArray
  }

  private val doParse = if (requiredSchema.nonEmpty) {
    (input: String) => convert(tokenizer.parseLine(input))
  } else {
    (_: String) => InternalRow.empty
  }

  override def parse(input: String): InternalRow = doParse(input)

  private def convert(tokens: Array[String]): InternalRow = {
    var i = 0
    while (i < requiredSchema.length) {
      row(i) = valueConverters(i).apply(tokens)
      i += 1
    }
    row
  }
}
