// register AtlasGeometryUDT and UserDefinedFunctions
import org.apache.spark.sql.atlas.AtlasRegister
AtlasRegister.init(spark)

// read DataFrame from Atlas csv file
val df = spark.read.format("atlas")
  .option("maxPartitionBytes", "134217728")
  .load("hdfs://129.82.208.136:9000/noaa-1-hour/csv-mod/2013")
df.createOrReplaceTempView("noaa")

// filter noaa DataFrame using field level restrictions
val spatialDf = df.filter("_c0 >= 40.78125 AND _c0 <= 41.484364
  AND _c1 >= -143.4375 AND _c1 <= -142.03127")

// parse pointDf from noaa DataFrame
val pointDf = spark.sql("SELECT BuildPoint(_c1, _c0) AS point, _c2, _c3 FROM noaa")
df.createOrReplaceTempView("pointDf")

// compute distances over pointDf
var distanceDf = spark.sql("SELECT point, Distance(point,
  BuildPoint(0.0, 10.0) as distance FROM pointDf WHERE distance < 10")
