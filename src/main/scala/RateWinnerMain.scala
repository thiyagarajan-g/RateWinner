package org.thi

import org.apache.spark._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object RateWinnerMain {

  def main(args: Array[String]) {

    //System.setProperty("hadoop.home.dir", "C:\\Users\\thi\\Downloads\\hadoop-3.2.2.tar\\hadoop-3.2.2")
    val c = 1.0
    val b = 1.0

    val hospitals = Array("hospital1", "hospital2", "hospital3", "hospital4", "hospital5")

    def generateRating = scala.util.Random.nextInt(10)
    def selectHospital = hospitals(scala.util.Random.nextInt(5))
    def generateBinary = scala.util.Random.nextInt(2)
    def computeWinnerRating1(p: Long, n: Long): Double = 1.0 * (p - c * n) / (1 + b * n / p)

    def generateRatingNew : Array[Int] = {
        val count = 5
        var sum = 10
        val g = new scala.util.Random()

        val vals = new Array[Int](count)
        sum = sum - count
        var i = 0

        for (i <- 0 to count-2 ) { vals(i) = g.nextInt(sum) }
        vals(count-1) = sum;
        scala.util.Sorting.quickSort(vals)
        for (i <- count-1 to 1 by -1) { vals(i) = vals(i) - vals(i-1) }
        for (i <- 0 to count-1) { vals(i) = vals(i) + 1 }
	vals
    }

    val spark = SparkSession.builder().appName("RateWinnerMain").master("local").getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    import sqlContext.implicits._
    val df = sc.parallelize(Seq.fill(4000) {
      val vals = generateRatingNew
      var list = List[(String, Int, Int)]()
      for ( i <- 0 to 4) {
      	val j = generateBinary
      	list  = list :+ (hospitals(i),  if(j == 0) vals(i) else 0, if(j == 1) vals(i) else 0)
      }
      list
    }.flatten, 10).toDF("Hospital", "P", "N")

    //df.write.mode(SaveMode.Overwrite).csv("file:/D:/rating.csv")
    df.show(10, false)

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val compute = df.groupBy("Hospital").agg(sum("P"), sum("N"))
    compute.show(10, false)
    compute.printSchema()

    val rdds = compute.rdd.map(row =>(row.getString(0), row.getLong(1), row.getLong(2), computeWinnerRating1(row.getLong(1), row.getLong(2))))

    val results = spark.createDataFrame(rdds).toDF("Hospital", "P", "N", "WinnerRating").sort(desc("WinnerRating"))
    results.show()
  }
}