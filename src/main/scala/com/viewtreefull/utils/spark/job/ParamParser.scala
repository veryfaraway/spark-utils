package com.viewtreefull.utils.spark.job

import scopt.OptionParser

abstract class ParamParser[C](programName: String) extends OptionParser[C](programName)
