package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX", (pickupPoint: String) => ((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY", (pickupPoint: String) => ((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ", (pickupTime: String) => ((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50 / HotcellUtils.coordinateStep
  val maxX = -73.70 / HotcellUtils.coordinateStep
  val minY = 40.50 / HotcellUtils.coordinateStep
  val maxY = 40.90 / HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  // Create a dataframe within min/max boundary of PickupInfo dataframe
  var DF_selected = pickupInfo.where(
      col("x") >= minX and
      col("x") <= maxX and
      col("y") >= minY and
      col("y") <= maxY and
      col("z") >= minZ and
      col("z") <= maxZ)
  .groupBy("x", "y", "z").count()

  DF_selected.createOrReplaceTempView("TempView_Hotcell")
  print("DF_selected")
  DF_selected.show()

  // Register UDF to check Neigborhood Cell
  spark.udf.register("UDF_neighborhoodCell", (
     x1: Int,
     x2: Int,
     y1: Int,
     y2: Int,
     z1: Int,
     z2: Int) =>
    (HotcellUtils.neighborhoodCell(x1, x2, y1, y2, z1, z2)))

  // Create Neighborhood Dataframe by compare between two HotCell dataframe
  var DF_neighborhoodCell = spark.sql(
    "select a.x as x, a.y as y, a.z as z, " +
    "sum(b.count) as SumNeighborhCell " +
    "from TempView_Hotcell a, " +
    "TempView_Hotcell b " +
    "where UDF_neighborhoodCell(a.x, b.x, a.y, b.y, a.z, b.z) " +
    "GROUP BY a.z, a.y, a.x")

    print("DF_neighborhoodCell")
    DF_neighborhoodCell.show()

  // Register UDF Number of Neighborhood cell calculation
  var UDF_NumOfadjacentCell = spark.udf.register(
  "UDF_neighborhoodCell", (
    minX: Int,
    minY: Int,
    minZ: Int,
    maxX: Int,
    maxY: Int,
    maxZ: Int,
    X: Int,
    Y: Int,
    Z: Int) =>
    (HotcellUtils.NumOfadjacentCell(minX, minY, minZ, maxX, maxY, maxZ, X, Y, Z)))

  // Create a Dataframe combination of Neighborhood cell and Number of Adjacent Cell cell
  var DF_Combine = DF_neighborhoodCell.withColumn(
    "NumOfNeiigborhoodCell",
    UDF_NumOfadjacentCell(
    lit(minX),
    lit(minY),
    lit(minZ),
    lit(maxX),
    lit(maxY),
    lit(maxZ),
    col("x"),
    col("y"),
    col("z")))

  // User defined function to calculate Z Score
  var UDF_ZScore = spark.udf.register(
    "UDF_ZScore",(
    x: Int,
    y: Int,
    z: Int,
    numCells: Int,
    hotCellAverage: Double,
    StandardDeviation: Double,
    NumOfNeiigborhoodCell: Int,
    SumNeighborhCell: Int
    ) =>
    (HotcellUtils.ZScore(x,y,z, numCells, hotCellAverage, StandardDeviation, NumOfNeiigborhoodCell, SumNeighborhCell )))

  // Calculate Hotcell average
  val hotCellAverage = DF_selected.agg(sum("count") / numCells).first.getDouble(0)

  // Calculate Standard deviation
  val StandardDeviation =  math.sqrt(DF_selected.agg(sum(pow("count", 2.0)) / numCells - math.pow(hotCellAverage, 2.0)).first.getDouble(0))

  // Create DF_ZScore dataframe. A combination between DF_Combine and ZScore.
  // Z Score is on a descending order.
  var DF_ZScore = DF_Combine.withColumn("ZScore", UDF_ZScore(col("x"), col("y"),col("z"), lit(numCells), lit(hotCellAverage), lit(StandardDeviation), col("NumOfNeiigborhoodCell"), col("SumNeighborhCell")))
    .orderBy(desc("ZScore"),desc("x"), asc("y"),desc("z"))

  var DF_Final = DF_ZScore.select(col("x"), col("y"), col("z"), col("Zscore")).limit(50)
  DF_Final.show()

  // Return Top 50 HotCell coordinate (x,y,z) without ZScore.
  return DF_Final.select(col("x"), col("y"), col("z"))

}
}
