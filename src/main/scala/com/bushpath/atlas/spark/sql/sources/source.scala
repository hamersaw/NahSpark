package com.bushpath.atlas.spark.sql.sources

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.sources.{DataSourceRegister, RelationProvider}

class AtlasDataSource extends DataSourceRegister with RelationProvider {
  override def shortName() = "atlas"

  override def createRelation(sqlContext: SQLContext,
      parameters: Map[String, String]) = {
    // initialize FileIndex
    val fileIndex = new InMemoryFileIndex(sqlContext.sparkSession,
      Seq(new Path(parameters("path"))), Map(), None)

    // initialize FileFormat
    val fileFormat = new CSVFileFormat()

    // infer schema
    //val dfs = new DistributedFileSystem()
    val files = fileIndex.inputFiles.map { path =>
      // TODO - use dfs to find file
      //dfs.getFileStatus(new Path(path))
      new FileStatus(0, false, 3, 128 * 1024 * 1024, 0, new Path(path))
    }

    val schema = fileFormat.inferSchema(sqlContext.sparkSession,
      Map("inferSchema" -> "true"), files)
    //val schema = fileFormat.inferSchema(sqlContext.sparkSession,
    //  Map(), files)

    // initialize AtlasRelation
    new AtlasRelation(sqlContext, 
      fileIndex, schema.orNull, fileFormat)
  }
}
