package com.viewtreefull.utils.common.lang

import com.viewtreefull.utils.common.lang.StringTools._
import org.scalatest.FunSuite

class StringToolsTest extends FunSuite {
  test("empty") {
    val nullString: String = null
    val emptyString: String = ""
    val spaceString: String = "    "
    val someString: String = "blah blah"

    assert(isEmpty(nullString))
    assert(isEmpty(emptyString))
    assert(isEmpty(spaceString))
    assert(!isEmpty(someString))

    assert(!isNotEmpty(nullString))
    assert(!isNotEmpty(emptyString))
    assert(!isNotEmpty(spaceString))
    assert(isNotEmpty(someString))
  }

}
