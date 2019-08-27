package com.viewtreefull.utils.spark.job

import scopt.OptionParser

/**
  * SampleDriverParam
  *
  * @author henry
  * @version 1.0
  * @since 2018. 1. 18.
  */
case class SampleDriverParam(input: String = "", output: String = "", option: Int = -1)
  extends JobParameter[SampleDriverParam] {

  projectName = "recommendation-common"
  projectVer = "v0.1"

  override def parseParams(parser: ParamParser[SampleDriverParam]): SampleDriverParam = {
    parser.opt[String]('i', "input").valueName("<db.table>")
      .action((x, c) => c.copy(input = x))
      .text("input is required")
      .required()

    parser.opt[String]('o', "output").valueName("<db.table>")
      .action((x, c) => c.copy(output = x))
      .text("output is required")
      .required()

    parser.opt[Int]("option")
      .action((x, c) => c.copy(option = x))
      .text("option is optional parameter")

    this
  }

}
