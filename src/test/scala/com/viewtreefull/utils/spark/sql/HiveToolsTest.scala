package com.viewtreefull.utils.spark.sql

import com.viewtreefull.utils.common.fs.FileFormat
import com.viewtreefull.utils.spark.test.SparkSessionTestWrapper
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class HiveToolsTest extends FunSuite with SparkSessionTestWrapper {

  import spark.implicits._

  val df: DataFrame = Seq(
    ("String", 1, 1L, 0.1F, 0.1D)
  ).toDF("string", "int", "long", "float", "double")

  test("create table query") {
    val expectedSql =
      s"""CREATE TABLE IF NOT EXISTS db.table
         |(
         |string STRING,
         |int INT,
         |long BIGINT,
         |float FLOAT,
         |double DOUBLE
         |)
         |STORED AS orc;""".stripMargin

    val sql = getTableCreationQuery(df, "db.table", None, FileFormat.orc)
    println(sql)

    assert(sql === expectedSql)
  }

  test("create partitioned table query") {
    val expectedSql =
      s"""CREATE TABLE IF NOT EXISTS db.table
         |(
         |string STRING,
         |int INT,
         |long BIGINT,
         |float FLOAT,
         |double DOUBLE
         |)
         |PARTITIONED BY (
         |dt STRING
         |)
         |STORED AS orc;""".stripMargin

    val partition = Seq(("dt", "20190901"))
    val sql = getTableCreationQuery(df, "db.table", Some(partition), FileFormat.orc)
    println(sql)

    assert(sql === expectedSql)
  }

  test("create partitioned table query2") {
    val expectedSql =
      s"""CREATE TABLE IF NOT EXISTS db.table
         |(
         |string STRING,
         |int INT,
         |long BIGINT,
         |float FLOAT,
         |double DOUBLE
         |)
         |PARTITIONED BY (
         |dt STRING,
         |hour INT
         |)
         |STORED AS orc;""".stripMargin

    val partition = Seq(("dt", "20190901"), ("hour", 22))
    val sql = getTableCreationQuery(df, "db.table", Some(partition), FileFormat.orc)
    println(sql)

    assert(sql === expectedSql)
  }


}
