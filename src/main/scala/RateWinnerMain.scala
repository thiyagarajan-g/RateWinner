package org.thi

import org.apache.spark._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object RateWinnerMain {

  def main(args: Array[String]) {

    //System.setProperty("hadoop.home.dir", "C:\\Users\\thi\\Downloads\\hadoop-3.2.2.tar\\hadoop-3.2.2")
    val c = 1.0
    val b = 1.0

    val candidates = Array("c1", "c2", "c3", "c4", "c5")

    def generateRating = scala.util.Random.nextInt(10)
    def selectCandidate = candidates(scala.util.Random.nextInt(5))
    def computeWinnerRating1(p: Long, n: Long): Double = 1.0 * (p - c * n) / (1 + b * n / p)

    val spark = SparkSession.builder().appName("RateWinnerMain").master("local").getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    import sqlContext.implicits._
    val df = sc.parallelize(Seq.fill(4000) {
      (selectCandidate, generateRating, generateRating)
    }, 10).toDF("Candidate", "P", "N")

    //df.write.mode(SaveMode.Overwrite).csv("file:/D:/rating.csv")
    df.show(10, false)

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val compute = df.groupBy("Candidate").agg(sum("P"), sum("N"))
    compute.show(10, false)
    compute.printSchema()

    val rdds = compute.rdd.map(row =>(row.getString(0), row.getLong(1), row.getLong(2), computeWinnerRating1(row.getLong(1), row.getLong(2))))

    val results = spark.createDataFrame(rdds).toDF("Candidate", "P", "N", "WinnerRating").sort(desc("WinnerRating"))
    results.show()
  }
}