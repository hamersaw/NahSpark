package com.bushpath.atlas.spark.sql.sources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.execution.datasources.{FileFormat, FileIndex, HadoopFsRelation}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class AtlasRelation(sqlContext: SQLContext, location: FileIndex,
    dataSchema: StructType, fileFormat: FileFormat)
    extends HadoopFsRelation(location, new StructType(),
      dataSchema, None, fileFormat, Map())(sqlContext.sparkSession)
    with PrunedFilteredScan {
  // TODO - implement needConversion
  //override def needConversion: Boolean = true

  // TODO - implement unhandledFilters
  //override def unhandledFilters(filters: Array[Filter]): Array[Filter] = filters

  // TODO - implement buildScan
  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {
    
    // TODO - use fileFormat.buildReader
    val rdds = inputFiles.map { path =>
      sqlContext.sparkContext.textFile(path).map { line => 
        val fields = line.split(",")
        Row.fromSeq(Seq(fields(0)))
      }
    }

    sqlContext.sparkContext.union(rdds)
  }
}
