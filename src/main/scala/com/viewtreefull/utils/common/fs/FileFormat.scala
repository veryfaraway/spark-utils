package com.viewtreefull.utils.common.fs

/**
  * FileFormat
  *
  * @author henry
  * @version 1.0
  * @since 03/04/2018
  */
object FileFormat extends Enumeration {
  type FileFormat = Value
  val sequencefile, rcfile, orc, parquet, textfile, avro = Value

  def getDefault: FileFormat = orc

  def getDefaultAsString: String = getDefault.toString

  def withNameWithDefault(name: String): FileFormat = {
    values.find(_.toString == name.toLowerCase()).getOrElse(getDefault)
  }
}
