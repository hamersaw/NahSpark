package com.bushpath.atlas.spark.sql.sources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class AtlasRelation(val sqlContext: SQLContext, val path: String)
    extends BaseRelation with PrunedFilteredScan {
  // TODO - implement schema
  override def schema = StructType(Seq(
    StructField("test", StringType, false)))

  // TODO - implement sizeInBytes
  //override def sizeInBytes: Long = sqlContext.conf.defaultSizeInBytes

  // TODO - implement needConversion
  //override def needConversion: Boolean = true

  // TODO - implement unhandledFilters
  //override def unhandledFilters(filters: Array[Filter]): Array[Filter] = filters

  // TODO - implement buildScan
  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {
    sqlContext.sparkContext.textFile(path).map { line => 
      val fields = line.split(",")
      Row.fromSeq(Seq(fields(0)))
    }
  }
}
