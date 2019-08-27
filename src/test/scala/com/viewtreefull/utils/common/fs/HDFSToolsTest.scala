package com.viewtreefull.utils.common.fs

import org.scalatest.FunSuite

class HDFSToolsTest extends FunSuite {

  test("test temp path") {
    val tempPath = FileTools.getTempPath("s3://", "test", "temp")
    println(tempPath)

    // time is changed every time
    assert(tempPath.startsWith("s3://test/temp/"))
  }

}
