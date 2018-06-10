package services

/**
  * Created by mshinde on 11-06-2018.
  */
object MovingAverageFunction {
  private var queue = new scala.collection.mutable.Queue[Double]()
  def movingAvg(n: Double)(implicit movingAvgPeriod: Int): Double = {
    queue.enqueue(n)
    if (queue.size > movingAvgPeriod)  queue.dequeue
    queue.sum / queue.size
  }
  override def toString = queue.mkString("(", ", ", ")")
  def clearQueue = queue.clear
}