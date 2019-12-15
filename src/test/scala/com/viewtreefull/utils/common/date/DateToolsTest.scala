package com.viewtreefull.utils.common.date

import java.time.LocalTime

import org.scalatest.FunSuite

class DateToolsTest extends FunSuite {
  test("time difference") {
    val t1 = LocalTime.of(1, 0, 0)
    val t2 = LocalTime.of(3, 0, 0)
    println(t1)
    println(t2)
    println(DateTools.getTimeDiff(t1, t2))
  }

  test("time difference 2") {
    val t1 = LocalTime.of(1, 50, 34)
    val t2 = LocalTime.of(3, 0, 0)
    println(t1)
    println(t2)
    println(DateTools.getTimeDiff(t1, t2))
  }

  test("time difference 3") {
    val t1 = LocalTime.of(1, 50, 34)
    val t2 = LocalTime.of(1, 53, 10)
    val diff = DateTools.getTimeDiff(t1, t2)
    assert(diff == (0, 2, 36))

    println(t1)
    println(t2)
    println(diff)
  }

}
