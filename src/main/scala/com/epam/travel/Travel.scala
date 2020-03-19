/*
package com.epam.travel

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Travel extends Configuration with App {
  println("hello Travel")

  println(spark)
  println("hello travellers")
  // val data = spark.read.format("csv").load("hdfs://nn01.itversity.com:8020/user/saikiranvishu/bids.txt").toDF("motelId","bidDate","HU","UK","NL","US","MX","AU","CA","CN","KR","BE","I","JP","IN","HN","GY","DE")
  val data = spark.read.schema(getSchema).format("csv").load("hdfs://nn01.itversity.com:8020/user/saikiranvishu/bids.txt")
  data.printSchema()
  data.show()
  data.createOrReplaceTempView("bids")
  val bids = spark.sql("select * from bids").toDF()
  //  val bids_error = spark.sql("select count(*) from bids where HU like '%ERROR%'").show()
  val q = spark.sql("select motelId,bidDate, count(*) as Error_Rec from bids where HU like 'ERROR%' group by motelId, bidDate ")
  q.write.option("header","true").mode(SaveMode.Overwrite).saveAsTable("saikiranvishu.bids")
  q.coalesce(1).write.option("header","true").mode(SaveMode.Overwrite).format("csv").save("hdfs://nn01.itversity.com:8020/user/saikiranvishu/bids_err.csv")
  val r = spark.sql("select * from bids where HU not like 'ERROR%'")
  r.write.option("header","true").mode(SaveMode.Overwrite).saveAsTable("saikiranvishu.bids1")
  r.write.option("header","true").mode(SaveMode.Overwrite).format("csv").save("hdfs://nn01.itversity.com:8020/user/saikiranvishu/bids.csv")

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


}
*/
