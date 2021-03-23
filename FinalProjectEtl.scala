package com.ccbst.spark

import com.ccbst.spark.transformation.FinalProjectTrm
import com.ccbst.spark.utils.SparkApp
import org.apache.spark.sql.functions.{col, _}

object FinalProjectEtl extends SparkApp with App {

  val locationDf = spark.read.json("src/main/resources/covid_data/phu_locations.json")
  val phuDf = spark.read.option("header", "true").option("inferSchema","true")
    .csv("src/main/resources/covid_data/cases_by_status_and_phu.csv")

  val firstDf = FinalProjectTrm.firstTransform(locationDf)
  val secondDf = FinalProjectTrm.secondTransform(locationDf)
  val thirdDf = FinalProjectTrm.thirdTransform(locationDf)
  val fourthDf = FinalProjectTrm.fourthTransform(locationDf)
  val fifthDf = FinalProjectTrm.fifthTransform(locationDf)
  val sixthDf = FinalProjectTrm.sixthTransform(locationDf)
  val seventhDf = FinalProjectTrm.seventhTransform(locationDf)
  val eighthDf = FinalProjectTrm.eighthTransform(locationDf)
  val ninthDf = FinalProjectTrm.ninthTransform(phuDf)
  val tenthDf = FinalProjectTrm.tenthTransform(phuDf)
  val elevenDf = FinalProjectTrm.elevenTransform(phuDf)
  val twelveDf = FinalProjectTrm.twelveTransform(phuDf)
  val thirteenthDf = FinalProjectTrm.thirteenthTransform(phuDf)
  val fourteenthDf = FinalProjectTrm.fourteenthTransform(phuDf)
  val fifteenthDf = FinalProjectTrm.fifteenthTransform(phuDf)

  fifteenthDf.show(10000, false)
  fifteenthDf.printSchema()

  val totalactiveDf= phuDf.select(col("PHU_NAME"),col("ACTIVE_CASES"),col("FILE_DATE"))
    .where(col("FILE_DATE").startsWith("2020"))
    .groupBy("PHU_NAME","ACTIVE_CASES").agg(max("FILE_DATE")as "max").orderBy(col("max")desc)
  val total = phuDf.count()
  totalactiveDf.select(col("PHU_NAME"), col("ACTIVE_CASES"))//.agg("ACTIVE_CASES").as("Sum Of Active Cases In Dec-2020"))
    .withColumn("PERCENTAGE",(col("ACTIVE_CASES") / total * 100))
    .show()
}
