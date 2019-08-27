package com.viewtreefull.utils.common.shell

import org.scalatest.FunSuite

class ShellToolsTest extends FunSuite {
  test("execute shell command") {
    val res = ShellTools.executeCommand("ls -al", "/")
    println(res)
  }

  test("execute shell command with output") {
    val res = ShellTools.executeCommandWithOutput("ls -al", "/")
    println(res)
  }

}
