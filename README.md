# atlas-spark-sql
## OVERVIEW
Spatio-temporal spark implementation with optimizations for reading dataframes from Atlas.

## COMMANDS
#### SPARK SHELL (SQL)
    // read dataframe from atlas csv file
    import com.bushpath.atlas.spark.sql.sources.AtlasRelation
    val df = spark.sqlContext.read.format("atlas").load("hdfs://127.0.0.1:9000/user/hamersaw")
    df.printSchema()
    df.show()

    // dataframe operations
    df.select("_c0").show()
    df.filter($"_c1" > 100).show()
    df.groupBy("_c2").count().show()

    // rename columns
    val lookupMap = Map("_c0" -> "latitude",
        "_c1" -> "longitude", "_c3" -> "timestamp")
    val named_df = lookupMap.foldLeft(df)(
        (acc, ca) => acc.withColumnRenamed(ca._1, ca._2))

    // register AtlasGeometryUDT and User Defined Functions
    import org.apache.spark.sql.atlas.AtlasRegister
    AtlasRegister.init(spark)

    // parase AtlasGeometryUDT
    df.createOrReplaceTempView("atlas_test")
    spark.sql("SELECT _c0 FROM global_temp.atlas_test").show()
    var spatialDf = spark.sql("SELECT BuildPoint(_c0, _c1) AS point, _c2, _c3 FROM atlas_test")

    spatialDf.createOrReplaceTempView("spatial_test")
    var distanceDf = spark.sql("SELECT point, Distance(point, BuildPoint(0.0, 10.0)) as distance, _c2, _c3 FROM spatial_test")

## REFERENCES
- https://github.com/locationtech/jts
    - https://locationtech.github.io/jts/javadoc/
- https://github.com/EsotericSoftware/kryo
    - https://github.com/eishay/jvm-serializers/wiki
- https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-udfs.html
    - https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-PartitionedFile.html
- https://datasystemslab.github.io/GeoSpark/tutorial/sql/
- http://blog.madhukaraphatak.com/introduction-to-spark-two-part-6/

## TODO
- conversion of Atlas Expressions to translatable spatiotemporal filters 
- support timestamp queries
