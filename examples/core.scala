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
val opDuration = System.currentTimeMillis - opStart

//df.createOrReplaceTempView("nah_test")
//var spatialDf = spark.sql("SELECT * FROM nah_test WHERE Distance(BuildPoint(_c0, _c1), BuildPoint(0.0, 10.0)) < 10")

println("df:" + dfDuration + " count:" + opDuration)
