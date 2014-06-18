package org.apache.spark.partial

private[spark] class ReduceEvaluator[T]( //TODO is this param necessary?
                                         totalOutputs: Int,
                                     reducerFunction: (T, T) => T)
  extends ApproximateEvaluator[T, T] {

   var endResult : Option[T] = None

  override def merge(outputId: Int, taskResult: T): Unit = {
    endResult match {
      case None => endResult = Some(taskResult)
      case _ => endResult = Some(reducerFunction(endResult.get,taskResult))
    }
  }

  override def currentResult(): T = {
    endResult.get
  }
}
