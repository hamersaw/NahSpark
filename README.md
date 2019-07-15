# atlas-spark-sql
## OVERVIEW
Spatio-temporal spark implementation with optimizations for reading dataframes from Atlas.

## COMMANDS
#### SPARK SHELL (SQL)
    // read dataframe from atlas csv file
    import com.bushpath.atlas.spark.sql.sources.AtlasRelation
    val df = spark.sqlContext.read.format("atlas").load("hdfs://127.0.0.1:9000/user/hamersaw")
    df.printSchema()
    df.show()

    // dataframe operations
    df.select("_c0").show()
    df.filter($"_c1" > 100).show()
    df.groupBy("_c2").count().show()

    // rename columns
    val lookupMap = Map("_c0" -> "latitude",
        "_c1" -> "longitude", "_c3" -> "timestamp")
    val named_df = lookupMap.foldLeft(df)(
        (acc, ca) => acc.withColumnRenamed(ca._1, ca._2))

    // register AtlasGeometryUDT and User Defined Functions
    import org.apache.spark.sql.atlas.AtlasRegister
    AtlasRegister.init(spark)

    // parase AtlasGeometryUDT
    df.createOrReplaceTempView("atlas_test")
    spark.sql("SELECT _c0 FROM global_temp.atlas_test").show()
    var spatialDf = spark.sql("SELECT BuildPoint(_c0, _c1) AS point, _c2, _c3 FROM atlas_test")

    spatialDf.createOrReplaceTempView("spatial_test")
    var distanceDf = spark.sql("SELECT point, Distance(point, BuildPoint(0.0, 10.0)) as distance, _c2, _c3 FROM spatial_test")

## DATAFRAME OPERATIONS
- group_by: spatial / time
- polygons: sifting operator -> filtering on a particular constraint
- set: intersection, union, distinct
- sampling: uniform sampling, stratified sampling (at least 50 observations from everything)
- merging datasets: epa and noaa -> tollerances between datasets
- joins: what does a join look like in this system

#### AGGREGATIONS
- TODO (https://spark.apache.org/docs/latest/sql-getting-started.html)
#### FUNCTIONS
- area(geometry) -> double
    computes the area of the geometry
- boundary(geometry) -> geometry
    returns the boundary of this geometry
- buffer(geometry, double) -> geometry
    computes a buffer area around the geometry with the given width
- centroid(geometry) -> point
    returns the centroid of the geometry
- convexHull(geometry) -> geometry
    computes the smallest convex polygon which contains all points in this geometry
- dimension(geometry) -> int
    returns the dimension of the geometry
- distance(geometry, geometry) -> double
    computes the minimum distance from geometry a to geometry b
- envelope(geometry) -> geometry
    returns the bounding box of this geometry
- envelopeInternal(geometry) -> geometry
    returns the envelope containing minimum x and y values in the geometry
- isSimple(geometry) -> boolean
    tests whether geometry is simple
- isValid(geometry) -> boolean
    tests whether geometry is valid
- length(geometry) -> double
    returns the length of the geometry
- normalize(geometry) -> geometry
    computes the normalized form of this geometry
- numPoints(geometry) -> int
    returns the number of points in the geometry

- difference(geometry, geometry) -> geometry
    computes a geometry representing the closure of the point set of points in geometry a and not in geometry b
- intersection(geometry, geometry) -> geometry
    computes point set which is common to both geometry a and geometry b
- symDifference(geometry, geometry) -> geometry
    computes symmetric difference of geometry a and geometry b
- union(geometry, geometry) -> geometry
    computes union of geometry a and geometry b

- contains(geometry, geometry) -> boolean
    tests whether geometry a is contained by geometry b
- covers(geometry, geometry) -> boolean
    tests whether geometry a covers geometry b
- crosses(geometry, geometry) -> boolean
    tests whether geometry a crosses geometry b
- disjoint(geometry, geometry) -> boolean
    tests whether geometry a is disjoint from geometry b
- equals(geometry, geometry) -> boolean
    tests whether geometry a is structurally and numericaly equal to geometry b
- equalsExact(geometry, geometry) -> boolean
    tests whether geometry a is exactly equal to geometry b
- equalsExact(geometry, geometry, tollerance) -> boolean
    tests whether geometry a is exactly equal to geometry b with the given tollerance
- equalsNorm(geometry, geometry) -> boolean
    tests whether geometry a is equal to geometry b in their normalized forms
- equalsTopo(geometry, geometry) -> boolean
    tests whether geometry a is topologically equal to geometry b
- intersects(geometry, geometry) -> boolean
    tests whether geometry a intersects geometry b
- overlaps(geometry, geometry) -> boolean
    tests whether geometry a overlaps geometry b
- touches(geometry, geometry) -> boolean
    tests whether geometry a touches geometry b
- within(geometry, geometry) -> boolean
    tests whether geometry a is within geometry b

## TODO
- everything
