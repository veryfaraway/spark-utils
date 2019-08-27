package com.viewtreefull.utils.spark.job

import com.viewtreefull.utils.common.lang.StringTools

/**
 * Parse job parameters from input args using scopt
 *
 * @see [[https://github.com/scopt/scopt scopt]]
 */
trait JobParameter[C] {
  // default values of project, override this values to suit your project
  var projectName: String = "scopt"
  var projectVer: String = "3.x"

  def parseParams(parser: ParamParser[C]): C

  def getParams[T](job: T, args: Seq[String]): C = {
    val parser = getParser(job)
    val init = parseParams(parser)

    parser.parse(args, init) match {
      case Some(params) =>
        println("OptionParser: Parsing args is successful!!")
        println(params)
        params
      case None =>
        // arguments are bad, error message will have been displayed
        sys.exit(1)
    }
  }

  protected def getParser[T](jobParam: T): ParamParser[C] = {
    val className = StringTools.getClassName(jobParam.getClass)
    val jobName = StringTools.getClassSimpleName(jobParam.getClass)

    getParser(className, jobName, projectName, projectVer)
  }

  protected def getParser(className: String, jobName: String,
                          projectName: String, projectVer: String): ParamParser[C] = {

    new ParamParser[C](className) {
      head(projectName, s"($projectVer)", jobName)
    }
  }
}
