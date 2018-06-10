package controllers

import java.beans.Encoder
import java.sql.Timestamp
import javax.inject.Inject

import scala.concurrent.Future
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import play.api.i18n.MessagesApi
import play.api.libs.json.{JsPath, Json, Reads}
import play.api.mvc.{Action, AnyContent, Controller}
import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import bootstrap.Init
import org.apache.spark
import org.apache.spark.sql.functions._
import services.Price
import spire.implicits

case class Point(time:String, price: String )

class ApplicationController @Inject()(implicit webJarAssets: WebJarAssets,
                                      val messagesApi: MessagesApi) extends Controller {

  def index: Action[AnyContent] =
    Action.async {
      import play.api.libs.json._
      import play.api.libs.functional.syntax._

      val weekQuery = s""" SELECT time, price FROM godzilla where time > current_timestamp - interval 1 week"""
      val sparkSession = Init.getSparkSessionInstance

      val result: DataFrame = sparkSession.sql(weekQuery)
      val rawJson = result.toJSON.collect().mkString("[",",","]")
      val json: JsValue = Json.parse(rawJson)


      implicit val priceReads: Reads[Point] = (
            (JsPath \\ "price").read[String] and
              (JsPath \\ "time").read[String]
        ) (Point.apply _)

      val rplyWith: Seq[Point] = (json).as[Seq[Point]]
      Future.successful(Ok(views.html.index(rplyWith)))
    }
   def priceHistory(period: String, rollingavg:Int ) = Action{
     val sparkSession = Init.getSparkSessionInstance
     val result: DataFrame= period match{
       case "week" => val weekQuery = s""" SELECT time, price FROM godzilla where time > current_timestamp - interval 1 week"""
                      sparkSession.sql(weekQuery)
       case "month" =>val monthQuery = s""" SELECT time, price FROM godzilla where time > current_timestamp - interval 1 month"""
         sparkSession.sql(monthQuery)
       case _ => sparkSession.sql("SELECT time, price FROM godzilla where false")
     }

    import sparkSession.implicits._
    val res:Dataset[Price] = result.map(r => Price(r.getTimestamp(0),r.getString(1).toDouble))

     val rowJosn = (if(rollingavg > 1){
       import services.MovingAverageFunction._
       implicit val movingAvgPeriod = rollingavg
       res.map(r => r match{case Price(time:Timestamp, price:Double) => Price(time, movingAvg(price))})
         } else res)
     .toJSON.collect()

     Ok(rowJosn.mkString("[",",","]"))
   }


  def priceHistoryPeriod(start: String, end: String, rollingAvg: Int) = Action {
    val date = """(\d\d\d\d)-(\d\d)-(\d\d)""".r
    val sparkSession = Init.getSparkSessionInstance
    val a = start match {
      case date(_*) => true
    }
    val b = end match {
      case date(_*) => true
    }

    val qry = if( a && b){
      s""" SELECT time , price FROM godzilla where time >= "$start" and time <= "$end" """
    }
    else s"""SELECT time, price FROM godzilla where false"""
    import sparkSession.implicits._
    val res:Dataset[Price] = sparkSession.sql(qry).map(r => Price(r.getTimestamp(0),r.getString(1).toDouble))

    val rowJosn = (if(rollingAvg > 1){
      import services.MovingAverageFunction._
      implicit val movingAvgPeriod = rollingAvg
      res.map(r => r match{case Price(time:Timestamp, price:Double) => Price(time, movingAvg(price))})
    } else res)
      .toJSON.collect()
//    Ok(s"start:$start end:$end rolling:$rollingAvg")
    Ok(rowJosn.mkString)
  }

  def priceForecast(days: Int, rollingAvg: Int) = Action {
    val sparkSession = Init.getSparkSessionInstance
    val doubVals = sparkSession.sql("select price from godzilla").rdd.map(r => r.getString(0)).map(_.toDouble)

    val ts = Vectors.dense{
      if(rollingAvg > 1){
      import services.MovingAverageFunction._
      implicit val movingAvgPeriod = rollingAvg
      doubVals.collect.map(movingAvg)
      }
        else
      doubVals.collect
    }

    val arimaModel = ARIMA.fitModel(1, 1, 1, ts)
//    println("coefficients: " + arimaModel.coefficients.mkString(","))
//    println(ts.toArray.mkString(","))
    val forecast:Vector = arimaModel.forecast(ts, days)//.toArray.drop(ts.size)

//    Ok(s" pedicted days $days with rolling$rollingAvg")
    Ok(s" pedicted days $days result $forecast")
  }

}



