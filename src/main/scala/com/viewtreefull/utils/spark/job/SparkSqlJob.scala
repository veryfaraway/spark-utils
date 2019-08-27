package com.viewtreefull.utils.spark.job

import org.apache.spark.sql.SparkSession

/**
  * Spark Sql Job
  */
trait SparkSqlJob extends App {

  def getSparkSession(enableDynamicPartition: Boolean = false): SparkSession = {
    val sparkSession = SparkSession.builder()
      .enableHiveSupport()
      .config("hive.support.concurrency", "true")

    if (enableDynamicPartition) {
      sparkSession
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
    }

    sparkSession.getOrCreate()
  }


  def getSparkSession(configs: Seq[(String, String)]): SparkSession = {
    val sparkSession = SparkSession.builder()
      .enableHiveSupport()

    configs.foreach { c =>
      sparkSession.config(c._1, c._2)
    }

    sparkSession.getOrCreate()
  }

}
