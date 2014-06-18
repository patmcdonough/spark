/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.partial

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.PartialJobListener

/**
 * A JobListener for an approximate single-result action, such as count() or non-parallel reduce().
 * This listener waits up to timeout milliseconds and will return a partial answer even if the
 * complete answer is not available by then.
 *
 * This class assumes that the action is performed on an entire RDD[T] via a function that computes
 * a result of type U for each partition, and that the action returns a partial or complete result
 * of type R. Note that the type R must *include* any error bars on it (e.g. see BoundedInt).
 */
private[spark] class PartialActionListener[T, U, R](rdd: RDD[T],
                                                    //TODO func doesn't look like it's used in here
                                                    func: (TaskContext, Iterator[T]) => U,
                                                    evaluator: ApproximateEvaluator[U, R],
                                                    minimumResultsPercentage: Double,
                                                    timeout: Option[Long] = None,
                                                    stopFunction: (Int, Int, Long) => Boolean)
  extends PartialJobListener[T, U, R] with Logging {

  def this(rdd: RDD[T],
           //TODO func doesn't look like it's used in here
           func: (TaskContext, Iterator[T]) => U,
           evaluator: ApproximateEvaluator[U, R],
           minimumResultsPercentage: Double,
           timeout: Option[Long]) = this (rdd, func, evaluator, minimumResultsPercentage, timeout, PartialActionListener.defaultStopFunction(_: Int, _: Int, _: Long, minimumResultsPercentage, timeout))

  def this(rdd: RDD[T],
           //TODO func doesn't look like it's used in here
           func: (TaskContext, Iterator[T]) => U,
           evaluator: ApproximateEvaluator[U, R],
           timeout: Option[Long],
           stopFunction: (Int, Int, Long) => Boolean) = this (rdd, func, evaluator, 1.0, timeout, stopFunction)

  if (timeout.isDefined) require(timeout.get > 0,
    "Timeout must be grater than 0")

  val startTime: Long = System.currentTimeMillis()
  val totalTasks = rdd.partitions.size
  var finishedTasks = 0
  var stop = false

  var failure: Option[Exception] = None
  var resultObject: Option[PartialResult[R]] = None

  override def taskSucceeded(index: Int, result: Any) {
    synchronized {
      evaluator.merge(index, result.asInstanceOf[U])
      finishedTasks += 1
      if (!keepProcessing) {
        resultObject.foreach(r => r.setFinalValue(evaluator.currentResult()))
        this.notifyAll()
      }
    }
  }


  override def jobFailed(exception: Exception) {
    synchronized {
      failure = Some(exception)
      this.notifyAll()
    }
  }

  /**
   * Waits for up to timeout milliseconds since the listener was created and then returns a
   * PartialResult with the result so far. This may be complete if the whole job is done.
   */
  def awaitResult(): PartialResult[R] = synchronized {
    while (!stop && keepProcessing) {
      this.wait(PartialActionListener.remainingWaitTime(timeout, startTime))
    }
    if (failure.isDefined) throw failure.get
    resultObject = Some(new PartialResult(evaluator.currentResult(), true))
    return resultObject.get
  }

  private def keepProcessing: Boolean = {
    stop =
      if (totalTasks == finishedTasks) true
      else try stopFunction(finishedTasks, totalTasks, startTime)
      catch {
        case e: Exception => failure = Some(e)
          true
      }
    !stop
  }
}

object PartialActionListener {

  def defaultStopFunction(finishedTasks: Int, 
                          totalTasks: Int,
                          startTime: Long,
                          minimumResultsPercentage: Double, 
                          timeout: Option[Long]): Boolean = {

    require(minimumResultsPercentage >= 0.0 && minimumResultsPercentage <= 1.0,
      s"minimumResultsPercentage of ${100 * minimumResultsPercentage}% is not > 0% and <= 100%")


    timeout match {
      case None => {
        finishedTasks >= (totalTasks * minimumResultsPercentage)
      }
      case Some(_) if remainingWaitTime(timeout, startTime) <= 0 && {
        finishedTasks >= (totalTasks * minimumResultsPercentage)
      } => true
      case Some(_) if remainingWaitTime(timeout, startTime) <= 0 && ! {
        finishedTasks >= (totalTasks * minimumResultsPercentage)
      } => throw new IncompleteResultsException(s"Timeout occurred prior to ${100 * minimumResultsPercentage}%")
      case _ => false
    }
  }

  private def remainingWaitTime(timeoutParam: Option[Long], startTimeParam: Long): Long = {
    timeoutParam match {
      case None => 0
      case Some(_) => val timeLeft = (startTimeParam + timeoutParam.get) - System.currentTimeMillis()
        if (timeLeft < 0) 0 else timeLeft
    }
  }


}

