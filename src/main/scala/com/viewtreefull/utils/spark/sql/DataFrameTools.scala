package com.viewtreefull.utils.spark.sql

import org.apache.spark.sql.DataFrame

object DataFrameTools {

  /**
    * reset DataFrame partition count to adjust output file count for performance
    *
    * @param numFiles number of partitions of df
    * @param df       DataFrame
    * @return DataFrame
    */
  def setOutputFileCount(numFiles: Int)(df: DataFrame): DataFrame = {
    if (numFiles > 0) df.coalesce(numFiles)
    else df
  }

}
