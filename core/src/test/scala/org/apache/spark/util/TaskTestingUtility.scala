package org.apache.spark.util

import org.apache.spark.Logging

import scala.util.control.Exception._

object TaskTestingUtility extends Logging {

  def control(line: Array[String]): String = {
    type AIOOBE = ArrayIndexOutOfBoundsException
    catching(classOf[AIOOBE]).opt(line(0)) match {
      case Some("fail") => fail(line)
      case Some("sleep") => sleep(line)
      case _ => succeed(line)
    }

  }

  def sleep(line: Array[String]) = {
    val time = line(1).toInt
    logDebug(s"sleeping $time")
    Thread.sleep(time)
    logDebug("done sleeping")
    line.last
  }

  def fail(line: Array[String]) = {
    val message = if (line.length > 1) line.drop(1).mkString(" ") else "Dummy throwable message"
    throw new UnknownError(message)
  }

  def succeed(line: Array[String]) = {
    line.last
  }
}
