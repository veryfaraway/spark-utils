package com.viewtreefull.utils.hadoop.io

import com.viewtreefull.utils.common.fs.HDFSTools
import org.scalatest.FunSuite

import scala.sys.process._

class HDFSToolsTest extends FunSuite {
  test("create check file") {
    val checkFile = "/tmp/_SUCCESS"
    HDFSTools.createEmptyFile(checkFile)

    val executable = "which hadoop".!
    if (executable == 0) {
      val rtn = s"hadoop fs -test -e $checkFile" !

      assert(rtn == 0)
    }
  }
}
