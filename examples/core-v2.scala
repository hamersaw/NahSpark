// register AtlasGeometryUDT and User Defined Functions
import org.apache.spark.sql.atlas.AtlasRegister
AtlasRegister.init(spark)

// read dataframe from atlas csv file
val df = spark.read.format("org.apache.spark.sql.atlas.datasources.v2.AtlasSource").load("hdfs://127.0.0.1:9000/user/hamersaw")

//df.createOrReplaceTempView("atlas_test")
//var spatialDf = spark.sql("SELECT * FROM atlas_test WHERE Distance(BuildPoint(_c0, _c1), BuildPoint(0.0, 10.0)) < 10")
