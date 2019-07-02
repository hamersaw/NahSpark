package com.bushpath.atlas.spark.sql.sources

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{DataSourceRegister, RelationProvider}

class AtlasDataSource extends DataSourceRegister with RelationProvider {
  override def shortName() = "atlas"

  override def createRelation(sqlContext: SQLContext,
      parameters: Map[String, String]) = {
    new AtlasRelation(sqlContext, parameters("path"))
  }
}
