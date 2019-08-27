package com.viewtreefull.utils.spark

import com.viewtreefull.utils.common.lang.StringTools
import org.apache.spark.sql.SparkSession

package object sql {

  def loadData(tableName: String, path: String, spark: SparkSession): Unit = {
    spark.sql(
      s"""
         |LOAD DATA INPATH "$path"
         |OVERWRITE INTO TABLE $tableName
       """.stripMargin)
  }

  def loadDataWithPartitions(tableName: String, partitions: Seq[(String, Any)], path: String,
                             spark: SparkSession, overwrite: Boolean = true): Unit = {

    val query =
      s"""
         |LOAD DATA INPATH "$path" ${if (overwrite) "OVERWRITE" else ""}
         |INTO TABLE $tableName
         |PARTITION (${StringTools.getKeyValueString(partitions)})
      """.stripMargin

    println(s"LOAD DATA:\n$query")
    spark.sql(query)
  }

}
