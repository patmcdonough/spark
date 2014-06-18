package org.apache.spark.examples

import scala.util.control.Exception._
import scala.util.Random

import org.apache.spark._


object PartialCompletion extends Logging{

  def main(args: Array[String]) {

    control()

    def runMain() {

      val conf = new SparkConf().setAppName("Partial Job")
      val sc = new SparkContext(conf)

      val successes = (1 to 20).map(s =>  s"Success $s")
      val sleeps = (1 to 1).map(s => s"sleep 20000 $s")
      val fails = (1 until 1).map(s => s"fail intentional fail")
      val tasks = Random.shuffle(successes ++ sleeps ++ fails)

      println("=== Starting with the following tasks ===")
      println(tasks.mkString("\n"))
      println("=========================================")


      val tasksRdd = sc.parallelize(tasks,4)
      val spark_home = sc.getSparkHome().get
      val task_results = tasksRdd.pipe(s"$spark_home/bin/run-example PartialCompletion stdin")

      val reducerFunction: (String, String) => String = {
        _ + ", " + _
      }

      println("=== Normal Return ===")
      println(task_results.reduce(reducerFunction))
      println("=====================")


      println("=== Using \"Partial Reduce\" at >40% ===")

      println(task_results.partialReduce(reducerFunction, 0.4))
//      task_results.partialReduce(reducerFunction, 0.4, 8000)
//      task_results.partialReduce(reducerFunction, new Policy())

      println("====================================")

      logDebug("Exiting successfully")

    }

    def control(line: Array[String] = args) {
      type AIOOBE = ArrayIndexOutOfBoundsException
      catching(classOf[AIOOBE]).opt(line(0)) match {
        case Some("stdin") => stdin()
        case Some("fail") => fail(line)
        case Some("sleep") => sleep(line)
        case None => runMain()
        case _ => succeed(line)
      }
    }

    def stdin() {
      for (line <- Iterator.continually(readLine()).takeWhile(_ != null)) {
        control(line.split(" "))
      }
    }

    //TODO undo your copy/paste programming from here to RDDSuite

    def sleep(line: Array[String]) = {
      val time = line(1).toInt
      logDebug(s"Sleeping for $time ms... ")
      Thread.sleep(time)
      println(line.last)
    }

    def fail(line: Array[String]) = {
      val message = if (line.length > 1) line.drop(1).mkString(" ") else "Dummy throwable message"
      logDebug(s"Throwing error: $message")
      throw new UnknownError(message)
    }

    def succeed(line: Array[String]) = {
      val values = line.last
      logDebug(s"Returning command arguments $values")
      println(values)
    }

  }
}
