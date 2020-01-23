# NahSpark
## OVERVIEW
NahSpark (Needle and Hand Spark) is a set of spatiotemporal Spark extensions implemented with optimizations for reading dataframes from NahFS.

## COMMANDS
#### SPARK SHELL (SQL)
    // read dataframe from Nah csv file
    import com.bushpath.nah.spark.sql.sources.NahRelation
    val df = spark.sqlContext.read.format("nah").load("hdfs://127.0.0.1:9000/user/hamersaw")
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

## REFERENCES
- https://github.com/locationtech/jts
    - https://locationtech.github.io/jts/javadoc/
- https://github.com/EsotericSoftware/kryo
    - https://github.com/eishay/jvm-serializers/wiki
- https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-udfs.html
    - https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-PartitionedFile.html
- https://datasystemslab.github.io/GeoSpark/tutorial/sql/
- http://blog.madhukaraphatak.com/introduction-to-spark-two-part-6/

## SPARK FILES FOR DEVELOPMENTAL HELP
#### LOGICAL PLAN MANIPULATION
- sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/
    - basicLogicalOperators.scala : case classes for logical plan nodes
- sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/
    - Optimizer.scala : line 948 outlines 'PushDownPredicate' rule

## TODO
- **Nah Expressions to translatable spatiotemporal filters**
    - necessary for enabling PredicatePushDown on everything
- spatial queries - kNN, spatial joins, etc
- support timestamp queries
