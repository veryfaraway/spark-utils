package com.viewtreefull.utils.spark.job

import org.scalatest.FunSuite

class JobParameterTest extends FunSuite {
  test("case: normal") {
    val input = "inputValue"
    val output = "outputValue"
    val option = 1

    val inputArgs = Seq("-i", input, "-o", output, "--option", option.toString)
    val param = SampleDriverParam().getParams(this, inputArgs)

    assert(input == param.input)
    assert(output == param.output)
    assert(option == param.option)
  }

  test("case: optional param") {
    val input = "inputValue"
    val output = "outputValue"

    val inputArgs = Seq("-i", input, "-o", output)
    val param = SampleDriverParam().getParams(this, inputArgs)

    assert(input == param.input)
    assert(output == param.output)
    assert(-1 == param.option)
  }

  test("case: param missing") {
    val inputArgs = Seq("-i", "inputValue")
    val param = SampleDriverParam().getParams(this, inputArgs)

    println(s"output: ${param.output}")
  }

  test("case: redundant param") {
    val inputArgs = Seq("-i", "inputValue", "-o", "outputValue", "--noNeed")
    val param = SampleDriverParam().getParams(this, inputArgs)

    println(s"output: ${param.output}")
  }
}
