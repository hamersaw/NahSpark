// ./bin/spark-shell --jars ~/Projects/NahSpark/build/libs/NahSpark-0.2.1-all.jar -i ~/Projects/NahSpark/examples/benchmark.scala

// initialize constants
val filterIterations = 100
val geohashLength = 4

// compute latitude and longitude intervals for geohash filters
val intervalCount = scala.math.pow(4.0, geohashLength)
val latitudeInterval = 180.0 / intervalCount
val longitudeInterval = 360.0 / intervalCount

// register NahGeometryUDT and User Defined Functions
import org.apache.spark.sql.nah.NahRegister
NahRegister.init(spark)

// read dataframe from nah csv file
val dfStart = System.currentTimeMillis
val df = spark.read.format("nah").load("hdfs://127.0.0.1:9000/noaa-1-hour/csv-mod/2013")
val dfDuration = System.currentTimeMillis - dfStart
println("df:" + dfDuration)

// iterate over spatial filtering
val opStart = System.currentTimeMillis
val rand = new scala.util.Random
for (i <- 1 to filterIterations) {
  // calculate random latitude interval
  val minLatitude = (latitudeInterval * rand.nextInt(intervalCount.toInt)) - 90.0
  val maxLatitude = minLatitude + latitudeInterval - .0001

  // calculate random longitude interval
  val minLongitude = (longitudeInterval * rand.nextInt(intervalCount.toInt)) - 180.0
  val maxLongitude = minLongitude + longitudeInterval - .0001

  val count = df.filter("_c0 >= " + minLongitude + " AND _c0 <= " + maxLongitude + " AND _c1 >= " + minLatitude + " AND _c1 <= " + maxLatitude).count()
  println(i + " : " + count)
}
val opDuration = System.currentTimeMillis - opStart
println("operation:" + opDuration)
