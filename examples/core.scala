// ./bin/spark-shell --jars ~/Projects/NahSpark/build/libs/NahSpark-0.1.6-all.jar

// register NahGeometryUDT and User Defined Functions
import org.apache.spark.sql.nah.NahRegister
NahRegister.init(spark)

// read dataframe from nah csv file
val dfStart = System.currentTimeMillis
val df = spark.read.format("org.apache.spark.sql.nah.datasources.v2.NahSource").load("hdfs://127.0.0.1:9000/user/hamersaw")
//val df = spark.read.format("csv").load("hdfs://127.0.0.1:9000/user/hamersaw")
val dfDuration = System.currentTimeMillis - dfStart

val opStart = System.currentTimeMillis
//df.filter("nahTimestamp > '1'").count
//df.filter("nahGeohash = '8bcc'").count
//df.select("_c43").map(x=>x.getString(0).toDouble).map(x=>x-(x%10)).groupBy("value").count().show();

//df.createOrReplaceTempView("nah_test")

//var spatialDf = spark.sql("SELECT * FROM nah_test WHERE Distance(BuildPoint(_c0, _c1), BuildPoint(0.0, 10.0)) < 10")

//var spatialDf = spark.sql("SELECT * FROM nah_test WHERE Within(BuildPoint(_c0, _c1), BuildPolygon(0.0, 0.0, 0.0, 10.0, 10.0, 10.0, 10.0, 0.0, 0.0, 0.0))")
//var spatialDf = spark.sql("SELECT * FROM nah_test WHERE 0.0 < _c0")
//val spatialDf = df.filter("_c0 >= 0.0")

//val spatialDf = df.filter("_c0 >= 41.484375 AND _c0 <= 42.18749 AND _c1 >= -143.4375 AND _c1 <= -142.03127") // 8bce
val spatialDf = df.filter("_c0 >= 40.78125 AND _c0 <= 41.484364 AND _c1 >= -143.4375 AND _c1 <= -142.03127") // 8bcc
//val spatialDf = df.filter("_c0 >= 11.25 AND _c0 <= 22.49999 AND _c1 >= -135 AND _c1 <= -112.50001") // 86
val count = spatialDf.count()
println("spatialDf.count() = " + count)

// parse NahGeometryUDT
//df.createOrReplaceTempView("nah_test")
//spark.sql("SELECT _c0 FROM global_temp.nah_test").show()
//var spatialDf = spark.sql("SELECT BuildPoint(_c0, _c1) AS point, _c2, _c3 FROM nah_test")

//spatialDf.createOrReplaceTempView("spatial_test")
//var distanceDf = spark.sql("SELECT point, Distance(point, BuildPoint(0.0, 10.0)) as distance, _c2, _c3 FROM spatial_test")

val opDuration = System.currentTimeMillis - opStart
println("df:" + dfDuration + " operation:" + opDuration)
