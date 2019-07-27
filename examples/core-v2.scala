// register AtlasGeometryUDT and User Defined Functions
import org.apache.spark.sql.atlas.AtlasRegister
AtlasRegister.init(spark)

// read dataframe from atlas csv file
val df = spark.read.format("org.apache.spark.sql.atlas.datasources.v2.AtlasSource").load("hdfs://127.0.0.1:9000/user/hamersaw")
