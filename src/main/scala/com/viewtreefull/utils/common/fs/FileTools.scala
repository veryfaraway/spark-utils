package com.viewtreefull.utils.common.fs

import com.google.common.annotations.VisibleForTesting
import com.viewtreefull.utils.common.shell.ShellTools
import org.apache.log4j.LogManager

object FileTools {
  private val log = LogManager.getRootLogger

  /**
   * attach time in millisecond to path to avoid duplication
   * ex) prefix/childPath1/childPath2/1234567889
   * caution) there is no dir. separator(/) between prefix and childPaths
   *
   * @param prefix     prefix of path
   * @param childPaths child paths
   * @return path combined with prefix, child paths and time
   */
  @VisibleForTesting
  def getTempPath(prefix: String, childPaths: String*): String = {
    s"$prefix${childPaths.mkString("/")}/${System.currentTimeMillis()}"
  }

  def removeDir(dirPath: String): Unit = {
    val res = ShellTools.executeCommand("hdfs dfs -rm -r -skipTrash", dirPath)
    log.info(s"remove $dirPath: $res")
  }

}
