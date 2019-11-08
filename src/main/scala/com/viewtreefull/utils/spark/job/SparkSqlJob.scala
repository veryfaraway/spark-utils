package com.viewtreefull.utils.spark.job

import org.apache.spark.sql.SparkSession

/**
  * Spark Sql Job
  */
trait SparkSqlJob extends App {

  def getSparkSession(enableDynamicPartition: Boolean = false,
                      enableObjectStoreSettings: Boolean = false,
                      enableOrcSettings: Boolean = false,
                      enableParquetSetting: Boolean = false): SparkSession = {
    val sparkSession = SparkSession.builder()
      .enableHiveSupport()
      .config("hive.support.concurrency", "true")

    if (enableDynamicPartition) {
      sparkSession
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
    }

    if (enableObjectStoreSettings) {
      sparkSession
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
    }

    if (enableOrcSettings) {
      sparkSession
        .config("spark.sql.orc.filterPushdown", "true")
        .config("spark.sql.orc.splits.include.file.footer", "true")
        .config("spark.sql.orc.cache.stripe.details.size", "10000")
        .config("spark.sql.hive.metastorePartitionPruning", "true")
    }

    if (enableParquetSetting) {
      sparkSession
        .config("spark.hadoop.parquet.enable.summary-metadata", "false")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.hive.metastorePartitionPruning", "true")
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
