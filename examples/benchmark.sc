// ./bin/spark-shell --jars ~/Projects/NahSpark/build/libs/NahSpark-0.2.1-all.jar -i ~/Projects/NahSpark/examples/benchmark.scala

// initialize constants
val filterIterations = 20
val geohashLength = 4
val (minLatitude, maxLatitude) = (20.0, 50.0)
val (minLongitude, maxLongitude) = (-125.0, -65.0)

// compute latitude and longitude intervals for geohash filters
val intervalCount = scala.math.pow(4.0, geohashLength)
val latitudeInterval = 180.0 / intervalCount
val longitudeInterval = 360.0 / intervalCount

val minLatitudeInterval =
  scala.math.ceil((minLatitude + 90.0) / latitudeInterval).toInt
val maxLatitudeInterval =
  scala.math.floor((maxLatitude + 90.0) / latitudeInterval).toInt

println("latitudeIntervals(" + minLatitudeInterval
  + ", " + maxLatitudeInterval + ")")

val minLongitudeInterval =
  scala.math.ceil((minLongitude + 180.0) / longitudeInterval).toInt
val maxLongitudeInterval =
  scala.math.floor((maxLongitude + 180.0) / longitudeInterval).toInt

println("longitudeIntervals(" + minLongitudeInterval
  + ", " + maxLongitudeInterval + ")")

// register NahGeometryUDT and User Defined Functions
import org.apache.spark.sql.nah.NahRegister
NahRegister.init(spark)

// read dataframe from nah csv file
val dfStart = System.currentTimeMillis
val df = spark.read.format("nah").option("maxPartitionBytes", "4194304").option("lookAheadBytes", "4096").load("hdfs://127.0.0.1:9000/noaa-1-hour/csv-mod/2013")
val dfDuration = System.currentTimeMillis - dfStart
println("df:" + dfDuration)

// iterate over spatial filtering
val opStart = System.currentTimeMillis
val rand = new scala.util.Random
var evaluationCount = 0
for (i <- 1 to filterIterations) {
  // calculate random latitude interval
  val latRand = minLatitudeInterval + rand.nextInt(maxLatitudeInterval - minLatitudeInterval)
  val minLatitude = (latitudeInterval * latRand) - 90.0
  val maxLatitude = minLatitude + latitudeInterval - .0001

  //println("latRand: " + latRand)

  // calculate random longitude interval
  val longRand = minLongitudeInterval + rand.nextInt(maxLongitudeInterval - minLongitudeInterval)
  val minLongitude = (longitudeInterval * longRand) - 180.0
  val maxLongitude = minLongitude + longitudeInterval - .0001

  //println("longRand: " + longRand)

  //println("longitude( " + minLongitude + "," + maxLongitude
  //  + ") latitude(" + minLatitude + ", " + maxLatitude + ")")
  val count = df.filter("_c0 >= " + minLongitude + " AND _c0 <= " + maxLongitude + " AND _c1 >= " + minLatitude + " AND _c1 <= " + maxLatitude).count()
  if (count != 0) {
    evaluationCount += 1
  }
}
val opDuration = System.currentTimeMillis - opStart
println(evaluationCount + " expression(s) evaluated")
println("operation:" + opDuration)
