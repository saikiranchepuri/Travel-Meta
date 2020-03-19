package com.epam.travel

import org.apache.log4j.Logger
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object Motels extends Configuration with App {
  val logger = Logger.getLogger(getClass.getName)

  val bidsPath = args(0)
  val exchangePath = args(1)
  val motelsPath = args(2)
  val outputBasePath = args(3)
  val outputCurrencyPath = args(4)
  val outputMotelsPath = args(5)

  processData(spark,bidsPath,motelsPath)

  def processData(spark: SparkSession,bidsPath: String,motelsPath: String) = {

    val rawBids : DataFrame = getRawBids(spark,bidsPath)

    val erroneousRecords : DataFrame = getErroneousRecords(rawBids)
    erroneousRecords.coalesce(1).write.mode(SaveMode.Overwrite).format(Constants.CSV_FORMATTER).save(outputBasePath)

    val exchangeRates: DataFrame = getExchangeRates(spark,exchangePath)

    val bids: DataFrame = getBids(rawBids,exchangeRates)
    bids.coalesce(1).write.mode(SaveMode.Overwrite).format(Constants.CSV_FORMATTER).save(outputCurrencyPath)

    val convertDate: UserDefinedFunction = convert_date

    val motels: DataFrame = getMotels(spark,motelsPath)

    val enriched: DataFrame = getEnriched(bids,motels)

    enriched.coalesce(1).write.mode(SaveMode.Overwrite).format(Constants.CSV_FORMATTER).save(args(5))

  }


  def getRawBids(spark: SparkSession,bidsPath: String): DataFrame = {
    val file = spark.read.schema(getSchema).format("csv").load(args(0))
    file.createOrReplaceTempView("bids")
    spark.sql("select * from bids").toDF()
  }


  def getErroneousRecords(rawBids: DataFrame): DataFrame = {

//    rawBids.sqlContext.sql("select motelId,bidDate,HU, count(*) as Err_records from bids where HU like 'ERROR%' group by motelId,bidDate,HU")
    rawBids.select("bidDate","HU").filter(col("HU").rlike("ERROR")).groupBy("bidDate","HU").agg(count(col("HU")))
  }


  def getExchangeRates(spark: SparkSession,exchangePath: String): DataFrame = {
    val exchnageRateFile = spark.read.schema(Constants.EXCHANGE_RATES_HEADER).format(Constants.CSV_FORMATTER).load(args(1))
    exchnageRateFile.createOrReplaceTempView("exchange_rates")
    spark.sql("select * from exchange_rates").toDF()

  }

  def getBids(rawBids: DataFrame,exchangeRates: DataFrame): DataFrame = {
    val usdEur = 0.803
    exchangeRates
      .select("ValidFrom","CurrencyName","CurrencyCode","ExchangeRate")
      .groupBy("ValidFrom","CurrencyName","CurrencyCode","ExchangeRate")
      .agg(bround(col("ExchangeRate")*usdEur,3)).as("usd_conversion")
  }

  def convert_date: UserDefinedFunction = {
    udf((func: String) => Constants.INPUT_DATE_FORMAT.parseDateTime(func).toString(Constants.OUTPUT_DATE_FORMAT))
  }

  def getMotels(session: SparkSession, motelsPath: String): DataFrame = {
    spark.read.schema(getMotelsSchema).format(Constants.CSV_FORMATTER).load(args(2))
    val motels = spark.read.schema(getMotelsSchema).format(Constants.CSV_FORMATTER).load(args(2))
    motels.createOrReplaceTempView("motels")
    spark.sql("select * from motels").toDF()
  }

  def getEnriched(bids: DataFrame,motels: DataFrame) : DataFrame = {
    bids.select("motelId","bidDate")
    motels.select("MotelId","MotelName","Country")
    motels.join(bids, motels("MotelId") === bids("motelId"))

  }

  def getSchema: StructType = {
    StructType(
      Array(
        StructField("motelId", StringType), StructField("bidDate", StringType, nullable = true), StructField("HU", StringType, nullable = true),
        StructField("UK", DoubleType, nullable = true), StructField("NL", DoubleType, nullable = true), StructField("US", DoubleType, nullable = true),
        StructField("MX", DoubleType, nullable = true), StructField("AU", DoubleType, nullable = true), StructField("CA", DoubleType, nullable = true),
        StructField("CN", DoubleType, nullable = true), StructField("KR", DoubleType, nullable = true), StructField("BE", DoubleType, nullable = true),
        StructField("I", DoubleType, nullable = true), StructField("JP", DoubleType, nullable = true), StructField("IN", DoubleType, nullable = true),
        StructField("HN", DoubleType, nullable = true), StructField("GY", DoubleType, nullable = true), StructField("DE", DoubleType, nullable = true)))
  }

  def getExchangeRateSchema : StructType = {
    StructType {
      Array( StructField("ValidFrom", StringType, nullable = true), StructField("CurrencyName", StringType, nullable = true),
        StructField("CurrencyCode", StringType, nullable = true),StructField("ExchangeRate", StringType, nullable = true)
      )
    }
  }

  def getMotelsSchema : StructType = {
    StructType {
      Array( StructField("MotelId", StringType, nullable = true), StructField("MotelName", StringType, nullable = true),
        StructField("Country", StringType, nullable = true),StructField("URL", StringType, nullable = true),
        StructField("Comment",StringType,nullable = true)
      )
    }
  }
}

// execution: spark-submit --class com.epam.travel.Motels travel-meta_2.11-0.1.jar /user/saikiranvishu/bids.txt /user/saikiranvishu/exchange_rate.txt /user/saikiranvishu/motels.txt
// /user/saikiranvishu/bids1.csv /user/saikiranvishu/exchange1.csv /user/saikiranvishu/motesl1.txt
