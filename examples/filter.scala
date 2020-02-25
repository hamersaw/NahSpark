// ./bin/spark-shell --jars ~/Projects/NahSpark/build/libs/NahSpark-0.2.1-all.jar

// register NahGeometryUDT and User Defined Functions
import org.apache.spark.sql.nah.NahRegister
NahRegister.init(spark)

// read dataframe from nah csv file
val dfStart = System.currentTimeMillis

val df = spark.read.format("nah").load("hdfs://127.0.0.1:9000/noaa-1-hour/csv-mod/2013")

val dfDuration = System.currentTimeMillis - dfStart
println("df:" + dfDuration)

val opStart = System.currentTimeMillis

// filter using spark SQL
//df.createOrReplaceTempView("noaa")
//var spatialDf = spark.sql("SELECT * FROM noaa WHERE Within(BuildPoint(_c0, _c1), BuildPolygon(0.0, 0.0, 0.0, 10.0, 10.0, 10.0, 10.0, 0.0, 0.0, 0.0))")

// filter using dataframe API
//val spatialDf = df.filter("_c0 >= 18.28125 AND _c0 <= 18.984365 AND _c1 >= -71.71875 AND _c1 <= -70.31251") // geohash 92d9
val spatialDf = df.filter("_c1 >= 18.28125 AND _c1 <= 18.984365 AND _c0 >= -71.71875 AND _c0 <= -70.31251") // geohash 92d9

val count = spatialDf.count()
println("spatialDf.count() = " + count)

val opDuration = System.currentTimeMillis - opStart
println("operation:" + opDuration)
