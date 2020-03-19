package com.epam.traveltest

import com.epam.travel.Motels
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class MotelsHomeRecommendationTest extends FunSuite {

  val spark = SparkSession.builder().master("yarn").appName("motelsRecommendation")
    .config("spark.sql.warehouse.dir","/apps/hive/warehouse").config("hive.metastore.uris","thrift://rm01.itversity.com:9083").enableHiveSupport().getOrCreate()


  test("reading raw bids") {
    val input_bids = "src/test/mockfiles/bids_sample.txt"
    val expected = spark.sparkContext.parallelize(Seq(List("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "", "1.35"),
      List("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL")))
    val rawBids = Motels.getRawBids(spark,input_bids)
    assert(expected == rawBids)
  }

  test("should collect erronerous records"){
    val rawBids = spark.sparkContext.parallelize(
      Seq(
        List("1", "06-05-02-2016", "ERROR_1"),
        List("2", "15-04-08-2016", "0.89"),
        List("3", "07-05-02-2016", "ERROR_2"),
        List("4", "06-05-02-2016", "ERROR_1"),
        List("5", "06-05-02-2016", "ERROR_2")
      )
    )

    val expected = spark.sparkContext.parallelize(Seq(
      "06-05-02-2016,ERROR_1,2",
      "06-05-02-2016,ERROR_2,1",
      "07-05-02-2016,ERROR_2,1"
    ))
    assert(rawBids === expected)
  }

  test("reading exchange data") {
    val exchange_rate = "src/test/mockfiles/exchange_rate_sample.txt"
    val expected = spark.sparkContext.parallelize(Seq(List("11-06-05-2016","Euro","EUR","0.803"),
      List("11-05-08-2016","Euro","EUR","0.873")))
    val exchangeRate = Motels.getRawBids(spark,exchange_rate)
    assert(expected == exchangeRate)
  }

  test("reading motels data") {
    val motels =  "src/test/mockfiles/motels.txt"
    val expected = spark.sparkContext.parallelize(Seq(List("0000001,Olinda Windsor Inn,IN,http://www.motels.home/?partnerID=3cc6e91b-a2e0-4df4-b41c-766679c3fa28,Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.")))
    val motelsDate = Motels.getMotels(spark,motels)
    assert(expected == motelsDate)
  }

}
