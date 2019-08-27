package com.viewtreefull.utils.common.lang

import java.util.regex.PatternSyntaxException

/**
  * StringTools
  *
  * @version 1.0
  * @since 2017. 7. 27.
  */
object StringTools {
  /** return simple name of class without last $ mark */
  def getClassSimpleName(aClass: Class[_]): String = {
    aClass.getSimpleName.split("\\$").last
  }

  def getClassName(aClass: Class[_]): String = {
    aClass.getName.split("\\$").last
  }

  def removeStringIgnoreCase(orig: String, pattern: String): String = {
    removeString(orig, "(?i)" + pattern)
  }

  def removeString(orig: String, regex: String): String = {
    try {
      orig.replaceAll(regex, "")
    } catch {
      case e: PatternSyntaxException =>
        println(s"${e.getMessage}: ${e.getDescription}")
        println(s"\torig=$orig, \n\tpattern=$regex")
        orig
    }
  }

  /**
    * return key1='value1',key2='value2',...
    *
    * @param keys   list of key
    * @param values list of value
    * @return
    */
  def getKeyValueString(keys: Seq[String], values: Seq[String]): String = {
    val str = StringBuilder.newBuilder
    for ((k, v) <- keys zip values) {
      str.append(s"$k='$v',")
    }

    str.deleteCharAt(str.size - 1).toString
  }

  // in: Seq((key1, value1),(key2, value2)), return key1='value1',key2='value2',...
  def getKeyValueString(keyValueList: Seq[(String, Any)]): String = {
    keyValueList.map {
      case (k: String, v: String) =>
        if (v.isEmpty) s"$k<>''"
        else s"$k='$v'"
      case (k: String, v: Any) => s"$k=$v"
    }.mkString(",")
  }

  def concatStrings(strings: Seq[String], delim: String = ","): String = {
    val builder = StringBuilder.newBuilder
    strings.foreach(s => builder.append(s).append(delim))

    // remove last delimiter
    builder.deleteCharAt(builder.length - 1)

    builder.toString()
  }

  def isNumeric(str: String): Boolean = {
    Option(str) match {
      case Some(s) => s.trim.forall(_.isDigit)
      case None => false
    }
  }

  /**
    * Returns separated partition values as a single string value
    *
    * @param partitions Hive table partition formatted like Seq(("dt", "20180901"), ("hour", "12"))
    * @return String for example, dt=20180901/hour=12
    */
  def concatPartitions(partitions: Seq[(String, String)]): String = {
    partitions.map(p => p.productIterator.mkString("=")).mkString("/")
  }

}
