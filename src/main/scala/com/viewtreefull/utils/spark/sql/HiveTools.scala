package com.viewtreefull.utils.spark.sql

import com.viewtreefull.utils.common.fs
import com.viewtreefull.utils.common.fs.FileFormat.FileFormat
import com.viewtreefull.utils.common.fs.{FileFormat, FileTools, HDFSTools}
import com.viewtreefull.utils.common.lang.StringTools
import org.apache.log4j.LogManager
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable

object HiveTools {
  private val log = LogManager.getLogger(this.getClass)

  /**
   * Return Maximum value of table partition to find latest value
   *
   * @param tableName dbName.tableName
   * @param spark     SparkSession
   * @return String value of partition like 'dt=20180821/hour=15'
   */
  def getMaxPartition(tableName: String, spark: SparkSession): String = {
    // col(partition)
    spark.sql(
      s"""
         |SHOW PARTITIONS $tableName
     """.stripMargin)
      .orderBy(col("partition").desc)
      .head().getString(0)
  }

  /**
   * Find Maximum value of table partition
   * And separate partition column and value as list of (column, value)
   * ex) if max partition is dt=20180821/hour=15 then return Seq((dt,20180821), (hour,15))
   *
   * @param tableName dbName.tableName
   * @param spark     SparkSession
   * @return list of (column, value)
   */
  def getMaxPartitionValues(tableName: String, spark: SparkSession): Seq[(String, String)] = {
    // col(partition)
    val maxPartitions = getMaxPartition(tableName, spark)

    maxPartitions.split("/").map { p =>
      val values = p.split("=")
      (values(0), values(1))
    }
  }

  /**
   * Find Maximum value of table partition
   * And separate partition column and value as Maps(Key: column, Value: value)
   * ex) if max partition is dt=20180821/hour=15 then return Map("dt" -> "20180821", "hour" -> "15")
   *
   * @param tableName dbName.tableName
   * @param spark     SparkSession
   * @return
   */
  def getMaxPartitionMaps(tableName: String, spark: SparkSession): mutable.Map[String, String] = {
    var partitionMap = scala.collection.mutable.Map[String, String]()

    // col(partition)
    val maxPartitions = getMaxPartition(tableName, spark)
    maxPartitions.split("/").foreach { p =>
      val values = p.split("=")
      partitionMap += (values(0) -> values(1))
    }

    partitionMap
  }

  /**
   * Save non-partitioned DataFrame as Hive table
   * Make sure that other tables don't refer to this table during writing
   * Because there is no locking table so if a job reads a table being written using this method,
   * can read incomplete data or fail to read
   *
   * @param df         DataFrame to save
   * @param tableName  Hive table name to save ex) "db.table"
   * @param fileFormat see [[fs.FileFormat]]
   */
  def saveAsTableSimple(df: DataFrame, tableName: String, numFiles: Int = 0,
                        fileFormat: FileFormat = FileFormat.getDefault): Unit = {
    df.transform(DataFrameTools.setOutputFileCount(numFiles))
      .write.format(fileFormat.toString)
      .mode(SaveMode.Overwrite).saveAsTable(tableName)
  }

  /**
   * save non-partitioned table data as file first with specific file format
   *
   * @param df         DataFrame to save
   * @param tableName  Hive table name to save ex) "db.table"
   * @param path       Temporary file path to save DataFrame
   * @param fileFormat see [[fs.FileFormat]]
   */
  def saveAsTable(df: DataFrame, tableName: String, path: String,
                  numFiles: Int = 0, fileFormat: FileFormat = FileFormat.getDefault): Unit = {

    val spark = df.sparkSession

    // to debug easily
    log.info(s"Write table[$tableName]")
    df.printSchema()

    // save data to temp path first
    saveAsFile(df, numFiles, fileFormat, path)

    // finally load data into table
    loadData(tableName, path, spark)
    log.info(s"DataFrame is loaded into Table[$tableName]")

    // delete empty directory after loading data
    FileTools.removeDir(path)
  }

  /**
   * save table data with partitions dynamically
   * make sure add configuration to SparkSession below
   * - hive.exec.dynamic.partition=true
   * - hive.exec.dynamic.partition.mode=nonstrict
   *
   * @param df         DataFrame to save as table
   * @param tableName  table name to save
   * @param partitions list of names of partition ex) Seq("dt", "hour")
   * @param fileFormat default value is orc
   */
  def saveAsTableWithPartitionDynamically(df: DataFrame, tableName: String, partitions: Seq[String], numFiles: Int = 0,
                                          fileFormat: FileFormat = FileFormat.getDefault): Unit = {

    val spark = df.sparkSession
    val strCols = StringTools.concatStrings(df.columns)
    val strPartition = StringTools.concatStrings(partitions)

    // Register the DataFrame as a Hive table
    df.transform(DataFrameTools.setOutputFileCount(numFiles))
      .createOrReplaceTempView("df")

    val query =
      s"""
         |INSERT OVERWRITE TABLE $tableName
         |PARTITION($strPartition)
         |SELECT $strCols
         |FROM df
    """.stripMargin

    // to debug
    println(s"[spark-util]HiveTools.saveAsTableWithPartitionDynamically table: $query")
    spark.sql(query)
  }

  /**
   * Create table if not exists
   *
   * @param df         DataFrame to create as Hive table
   * @param tableName  table name ex) db.table
   * @param partitions Seq((colName, value), ...) value is referred to define data type
   * @param fileFormat default is orc
   */
  def createTable(df: DataFrame, tableName: String, partitions: Option[Seq[(String, Any)]],
                  fileFormat: FileFormat = FileFormat.getDefault): Unit = {
    val spark = df.sparkSession
    spark.sql(getTableCreationQuery(df, tableName, partitions, fileFormat))
  }

  /**
   * save table data with specific partitions
   * [IMPORTANT]partition column should not be included in DataFrame
   * because generated partition as directory by "LOAD DATA ..." query
   *
   * @param df         DataFrame to save as table
   * @param tableName  table name to save
   * @param partitions partitions of table as list of (column, value)
   * @param path       file location to load
   * @param numFiles   output file count to reduce files
   * @param overwrite  WriteMode, true: overwrite, false: append
   * @param flag       if flag is true, create _SUCCESS file
   * @param fileFormat default value is orc
   */
  def saveAsTableWithPartition(df: DataFrame, tableName: String, partitions: Seq[(String, Any)],
                               path: String, numFiles: Int = 0,
                               overwrite: Boolean = true, flag: Boolean = false,
                               fileFormat: FileFormat = FileFormat.getDefault): Unit = {

    val spark = df.sparkSession

    println(s"Save DataFrame as $tableName")
    df.printSchema()

    log.info(s"Create table if not exists: $tableName")
    createTable(df, tableName, Some(partitions), fileFormat)

    // to debug easily
    println(
      s"""
         |Write table[$tableName]:
         |\tpartitions - $partitions
         |\tOverwrite - $overwrite
         |\tcreate(_SUCCESS) - $flag
         |\tfromPath - $path
         |${df.printSchema}
       """.stripMargin)

    log.info(s"Save data to temp path first: $path")
    saveAsFile(df, numFiles, fileFormat, path)
    log.info("saveAsFile is done!")

    // finally load data into table
    log.info(s"Load files into Table[$tableName]")
    loadDataWithPartitions(tableName, partitions, path, spark, overwrite)
    log.info(s"loadData is done!")

    if (flag) {
      createCheckFile(tableName, partitions, spark)
      log.info("_SUCCESS file is created")
    }

    // delete empty directory after loading data
    log.info("Delete tempDir")
    HDFSTools.removeDir(path)
  }

  def saveAsFile(df: DataFrame, numFiles: Int, fileFormat: FileFormat, path: String): Unit = {
    df.transform(DataFrameTools.setOutputFileCount(numFiles))
      .write.format(fileFormat.toString).save(path)
    log.info(s"DateFrame is saved at [$path]")
  }

  // create _SUCCESS file
  def createCheckFile(tableName: String, partitions: Seq[(String, Any)], spark: SparkSession): Unit = {
    val checkFileName = getCheckFileName(tableName, partitions, spark)
    println(s"CreateCheckFile: $checkFileName")
    if (checkFileName.isDefined) HDFSTools.createEmptyFile(checkFileName.get)
  }

  // get check file(_SUCCESS) absolute path
  def getCheckFileName(tableName: String, partitions: Seq[(String, Any)], spark: SparkSession): Option[String] = {
    val inputFiles = spark.table(tableName).select(input_file_name).take(1)
    if (inputFiles.length != 1) return None

    Some(s"${getTablePathWithPartitions(inputFiles(0).getString(0), tableName, partitions)}/_SUCCESS")
  }

  // partition 포함된 table 데이터 파일 경로 가져오기
  def getTablePathWithPartitions(samplePath: String, tableName: String, partitions: Seq[(String, Any)]): String = {
    val tableNamePath = if (tableName.contains(".")) tableName.replaceAll("\\.", "/") else tableName
    val index = samplePath.indexOf(tableNamePath)
    val tablePath = samplePath.substring(0, index + tableNamePath.length)

    val partitionPath = partitions.map {
      case (k: String, v: Any) => s"$k=$v"
    }.mkString("/")

    s"$tablePath/$partitionPath"
  }
}
