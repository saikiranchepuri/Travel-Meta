package com.epam.sales

import org.apache.spark.sql.SparkSession

trait Configuration {
  val spark = SparkSession.builder().master("yarn").appName("SalesInfo")
    .config("spark.sql.warehouse.dir","/apps/hive/warehouse").config("hive.metastore.uris","thrift://rm01.itversity.com:9083").enableHiveSupport().getOrCreate()
}