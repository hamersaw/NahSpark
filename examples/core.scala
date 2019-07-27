// register AtlasGeometryUDT and User Defined Functions
import org.apache.spark.sql.atlas.AtlasRegister
AtlasRegister.init(spark)

// read dataframe from atlas csv file
import org.apache.spark.sql.atlas.datasources.AtlasRelation
val df = spark.sqlContext.read.format("atlas").load("hdfs://127.0.0.1:9000/user/hamersaw")
//val df = spark.sqlContext.read.format("csv").load("hdfs://127.0.0.1:9000/user/hamersaw")

df.printSchema()

// parase AtlasGeometryUDT
//df.createOrReplaceTempView("atlas_test")
//var spatialDf = spark.sql("SELECT BuildPoint(_c0, _c1) AS point, _c2, _c3 FROM atlas_test")

//spatialDf.createOrReplaceTempView("spatial_test")
//var distanceDf = spark.sql("SELECT point, Distance(point, BuildPoint(0.0, 10.0)) as distance, _c2, _c3 FROM spatial_test WHERE Distance(point, BuildPoint(0.0, 10.0)) > 50")
