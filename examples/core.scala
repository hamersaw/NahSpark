// register AtlasGeometryUDT and User Defined Functions
import org.apache.spark.sql.atlas.AtlasRegister
AtlasRegister.init(spark)

// read dataframe from atlas csv file
val dfStart = System.currentTimeMillis
val df = spark.read.format("org.apache.spark.sql.atlas.datasources.v2.AtlasSource").load("hdfs://127.0.0.1:9000/user/hamersaw")
//val df = spark.read.format("csv").load("hdfs://127.0.0.1:9000/user/hamersaw")
val dfDuration = System.currentTimeMillis - dfStart

val opStart = System.currentTimeMillis
//df.filter("atlasTimestamp > '1'").count
//df.filter("atlasGeohash = '8bcc'").count
//df.select("_c43").map(x=>x.getString(0).toDouble).map(x=>x-(x%10)).groupBy("value").count().show();
val opDuration = System.currentTimeMillis - opStart

//df.createOrReplaceTempView("atlas_test")
//var spatialDf = spark.sql("SELECT * FROM atlas_test WHERE Distance(BuildPoint(_c0, _c1), BuildPoint(0.0, 10.0)) < 10")

println("df:" + dfDuration + " count:" + opDuration)
