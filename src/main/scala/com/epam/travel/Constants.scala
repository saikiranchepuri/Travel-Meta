package com.epam.travel

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.joda.time.format.DateTimeFormat

object Constants {
  val DELIMITER = "."
  val CSV_FORMATTER = "com.databricks.spark.csv"

  val EXCHANGE_RATES_HEADER = StructType(Array("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate")
    .map(field => StructField(field, StringType, true)))
  val TARGET_LOSAS = Seq("US", "CA", "MX")

  val INPUT_DATE_FORMAT = DateTimeFormat.forPattern("HH-dd-MM-yyyy")
  val OUTPUT_DATE_FORMAT = DateTimeFormat.forPattern("HH-dd-MM-yyyy HH:mm")

}
