package com.trendyol.bootcamp.flink.homework

import com.trendyol.bootcamp.flink.common._

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

case class PurchaseLikelihood(userId: Int, productId: Int, likelihood: Double)

object LikelihoodToPurchaseCalculator  {

    val l2pCoefficients = Map(
      AddToBasket -> 0.4,
      RemoveFromBasket -> -0.2,
      AddToFavorites -> 0.7,
      RemoveFromFavorites -> -0.2,
      DisplayBasket -> 0.5
    )

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val keyedStream = env
      .addSource(new RandomEventSource)
      .filter(e => List(AddToBasket, DisplayBasket, AddToFavorites, RemoveFromBasket, RemoveFromFavorites).contains(e.eventType))
      // Some event wasn't involved which has no effect on purchaselikelihood
      .keyBy(x => (x.userId,x.productId))
      //KeyBy userdID and productId this way i can calculate the number of event relativeley this user and the product which user has interest.

    keyedStream
      .window(TumblingProcessingTimeWindows.of(Time.milliseconds(20000))) //20 second timewindow, with keyby and window, user and product grouped and ready to calculate purchaselikelihood
      .process(new ProcessWindowFunction[Event,PurchaseLikelihood,(Int,Int),TimeWindow] {
        override def process(
                              key: (Int,Int),
                              context: Context,
                              elements: Iterable[Event],
                              out: Collector[PurchaseLikelihood]): Unit = {

          //Process function takes key(userId,ProductId), Iterable element(Events) and collection for output Purchaselikelihood.
          // We need to return Purchaselikelihood object which has three attritubte, userId,productId and likelihood.
          //Likelihood is calculating by fold function. Each event has different coefficient, foldLeft aggregate all events likelihood and return final double output
          val likelihood = elements.foldLeft(0D) { (acc, elem) =>
            (acc + l2pCoefficients.get(elem.eventType).getOrElse(0D))

          }
          //Last step, collect the output and add printSinkFunction that help us for seeing logs.
          out.collect(PurchaseLikelihood(key._1,key._2,BigDecimal.apply(likelihood).setScale(2,BigDecimal.RoundingMode.HALF_UP).toDouble))
        }

      })
      .addSink(new PrintSinkFunction[PurchaseLikelihood])

    // execute program
    env.execute("Likelihood to Purchase Calculator")
  }


}

