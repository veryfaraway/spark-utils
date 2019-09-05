package com.viewtreefull.utils.common.fs

import org.apache.log4j.LogManager

import sys.process._

object ShellTools {
  private val log = LogManager.getRootLogger

  def executeCommand(commands: String*): Int = {
    val cmd = commands.mkString(" ")
    log.debug(s"execute command: $cmd")
    cmd.!
  }

  def executeCommandWithOutput(commands: String*): String = {
    val cmd = commands.mkString(" ")
    log.debug(s"execute command: $cmd")
    cmd.!!
  }

}
