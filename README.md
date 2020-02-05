# NahSpark
## OVERVIEW
NahSpark (Needle and Hand Spark) is a set of spatiotemporal Spark extensions implemented with optimizations for reading dataframes from NahFS.

## COMMANDS
#### SPARK SHELL (SQL)
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

## TODO
- __implement spatial joins__
- support timestamp queries
