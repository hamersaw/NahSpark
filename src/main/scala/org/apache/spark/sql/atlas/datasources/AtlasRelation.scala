package org.apache.spark.sql.atlas.datasources

import com.bushpath.anamnesis.ipc.rpc.RpcClient

import com.univocity.parsers.csv.CsvParser

import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.spark.input.{PortableDataStream, StreamInputFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.{BinaryFileRDD, RDD}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.execution.FileRelation
import org.apache.spark.sql.execution.datasources.CodecStreams
import org.apache.spark.sql.execution.datasources.csv.CSVOptions
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable.ListBuffer

class AtlasRelation(atlasSqlContext: SQLContext, files: Array[String],
    size: Long, atlasSchema: StructType, fileFormatString: String)
    extends BaseRelation with FileRelation
      with PrunedFilteredScan with Serializable {

  // TODO - implement needConversion
  //override def needConversion: Boolean = true

  // TODO - implement unhandledFilters
  //override def unhandledFilters(filters: Array[Filter]): Array[Filter] = filters

  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {
    println("FILTERS")
    for (filter <- filters) {
      println("FILTER: '" + filter + "'")
    }

    // declare sparkSession
    val sparkSession = atlasSqlContext.sparkSession
    //val columnPruning = sparkSession.sessionState.conf.csvColumnPruning
    val columnPruning = false // TODO - setting to false ensures csv parsing returns fields ordered by requiredSchema

    val timeZone = sparkSession.sessionState.conf.sessionLocalTimeZone

    // initialize CSVOptions instance
    val csvOptions = new CSVOptions(Map("delimiter" -> ","), 
      columnPruning, timeZone)

    // compile required schema
    var requiredStructFields = new ListBuffer[StructField]()
    for (requiredColumn <- requiredColumns) {
      requiredStructFields += atlasSchema.apply(requiredColumn)
    }

    val requiredSchema = StructType(requiredStructFields)

    //println("SCHEMA: " + atlasSchema)
    //println("REQUIRED SCHEMA: " + requiredSchema)

    val paths = files.map(x => new Path(x))
    val name = paths.mkString(",")
    val job = Job.getInstance(sparkSession.sessionState
      .newHadoopConfWithOptions(csvOptions.parameters))
    FileInputFormat.setInputPaths(job, paths: _*)
    val conf = job.getConfiguration

    // construct binary rdd
    val baseRdd = new BinaryFileRDD(
      sparkSession.sparkContext,
      classOf[StreamInputFormat],
      classOf[String],
      classOf[PortableDataStream],
      conf,
      sparkSession.sparkContext.defaultMinPartitions)

    // Only returns `PortableDataStream`s without paths.
    val rdd2 = baseRdd.setName(s"CSVFile: $name").values

    val rdd3 = rdd2.flatMap { stream =>
      val codecStream = CodecStreams.createInputStreamWithCloseResource(
          stream.getConfiguration, new Path(stream.getPath()))

      AtlasCsvParser.parseStream(codecStream,
        atlasSchema, requiredSchema, csvOptions)
    }

    rdd3.map(row => Row.fromSeq(row.toSeq(requiredSchema)))
  }

  override def inputFiles: Array[String] = files

  override def sizeInBytes: Long = size

  override def schema: StructType = atlasSchema

  override def sqlContext: SQLContext = atlasSqlContext
}
