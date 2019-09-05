package com.viewtreefull.utils.common.fs

import com.google.common.annotations.VisibleForTesting
import org.apache.log4j.LogManager

import sys.process._

object HDFSTools {
  private val log = LogManager.getRootLogger

  @VisibleForTesting
  def createEmptyFile(fileName: String): Unit = {
    try {
      s"hadoop fs -touchz $fileName".!!
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def removeDir(tempPath: String): Unit = {
    val res = ShellTools.executeCommand("hdfs dfs -rm -r -skipTrash", tempPath)
    log.info(s"remove $tempPath: $res")
  }

}
