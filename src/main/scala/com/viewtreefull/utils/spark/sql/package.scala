package com.viewtreefull.utils.spark

import com.google.common.annotations.VisibleForTesting
import com.viewtreefull.utils.common.fs.FileFormat.FileFormat
import com.viewtreefull.utils.common.lang.StringTools
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

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

  def getTypeString(dataType: DataType): String = dataType match {
    case ShortType => "SMALLINT"
    case IntegerType => "INT"
    case LongType => "BIGINT"
    case FloatType => "FLOAT"
    case DoubleType => "DOUBLE"
    case BooleanType => "BOOLEAN"
    case StringType => "STRING"
    case BinaryType => "BINARY"
    case TimestampType => "TIMESTAMP"
    case DateType => "DATE"
    case at: ArrayType => s"ARRAY <${getTypeString(at.elementType)}>"
    case mt: MapType => s"MAP <${getTypeString(mt.keyType)}, ${getTypeString(mt.valueType)}>"
    case _ => throw new RuntimeException(s"Type ${dataType.typeName} is not supported!")
  }

  def getTypeString(value: Any): String = value match {
    case _: Int => "INT"
    case _: Long => "BIGINT"
    case _: Float => "FLOAT"
    case _: Double => "DOUBLE"
    case _: Boolean => "BOOLEAN"
    case _: String => "STRING"
    case x: Seq[Any] => s"ARRAY <${getTypeString(x)}>"
    case _ => throw new RuntimeException(s"Type ${value.getClass} is not supported!")
  }

  def getColumnSchemaQuery(df: DataFrame): String = {
    s"""
       |(
       |${df.schema.map(c => s"${c.name} ${getTypeString(c.dataType)}").mkString(",\n")}
       |)
       |""".stripMargin
  }

  def getPartitionCreationQuery(partitions: Seq[(String, Any)]): String = {
    val columnSchema = partitions.map { p =>
      s"${p._1} ${getTypeString(p._2)}"
    }.mkString(",\n")

    s"""PARTITIONED BY (
       |$columnSchema
       |)
       |""".stripMargin
  }

  @VisibleForTesting
  def getTableCreationQuery(df: DataFrame, tableName: String, partitions: Option[Seq[(String, Any)]],
                            fileFormat: FileFormat): String = {

    val sql = new mutable.StringBuilder("CREATE TABLE IF NOT EXISTS ")
      .append(tableName)
      .append(getColumnSchemaQuery(df))

    if (partitions.isDefined) sql.append(getPartitionCreationQuery(partitions.get))
    sql.append("STORED AS ").append(fileFormat)

    sql.toString
  }
}
