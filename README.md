# atlas-spark-sql
## OVERVIEW
Spatio-temporal spark implementation with optimizations for reading dataframes from Atlas.

## COMMANDS
#### SPARK SHELL (SQL)
    // read dataframe from atlas csv file
    import com.bushpath.atlas.spark.sql.sources.AtlasRelation
    val df = spark.sqlContext.read.format("atlas") \
        .load("hdfs://127.0.0.1:9000/user/hamersaw")
    df.printSchema()
    df.show()

    // dataframe operations
    df.select("_c0").show()
    df.filter($"_c1" > 100).show()
    df.groupBy("_c2").count().show()

    // use spark sql
    df.createGlobalTempView("atlas_test")
    spark.sql("SELECT _c0 FROM global_temp.atlas_test").show()

    // rename columns
    val lookupMap = Map("_c0" -> "latitude",
        "_c1" -> "longitude", "_c3" -> "timestamp")
    val named_df = lookupMap.foldLeft(df)(
        (acc, ca) => acc.withColumnRenamed(ca._1, ca._2))

## DATAFRAME OPERATIONS
- group_by: spatial / time
- polygons: sifting operator -> filtering on a particular constraint
- set: intersection, union, distinct
- sampling: uniform sampling, stratified sampling (at least 50 observations from everything)
- merging datasets: epa and noaa -> tollerances between datasets
- joins: what does a join look like in this system

#### AGGREGATIONS
- TODO (https://spark.apache.org/docs/latest/sql-getting-started.html)
#### FILTERING
- TODO

## TODO
- everything
