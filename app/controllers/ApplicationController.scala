package controllers

import java.sql.Timestamp
import javax.inject.Inject

import bootstrap.Init
import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Dataset}
import play.api.i18n.MessagesApi
import org.apache.spark.sql.functions._
import play.api.mvc.{Action, AnyContent, Controller}
import services.Price

import scala.concurrent.Future

/**
  * Created by mshinde on 11-06-2018.
  */
case class Point(time: String, price: String)

class ApplicationController @Inject()(implicit webJarAssets: WebJarAssets,
                                      val messagesApi: MessagesApi) extends Controller {

  def index: Action[AnyContent] =
    Action.async {
      import play.api.libs.functional.syntax._
      import play.api.libs.json._

      val selQuery = s""" SELECT time, price FROM godzilla """
      val sparkSession = Init.getSparkSessionInstance

      val result: DataFrame = sparkSession.sql(selQuery)
      val rawJson = result.sort(desc("time"))
        .toJSON.collect().mkString("[", ",", "]")
      val json: JsValue = Json.parse(rawJson)


      implicit val priceReads: Reads[Point] = (
        (JsPath \\ "price").read[String] and
          (JsPath \\ "time").read[String]
        ) (Point.apply _)

      val rplyWith: Seq[Point] = (json).as[Seq[Point]]
      Future.successful(Ok(views.html.index(rplyWith)))
    }

  def priceHistory(period: String, rollingavg: Int) = Action {
    val sparkSession = Init.getSparkSessionInstance
    val result: DataFrame = period match {
      case "week" => val weekQuery = s""" SELECT time, price FROM godzilla where time > current_timestamp - interval 1 week"""
        sparkSession.sql(weekQuery)
      case "month" => val monthQuery = s""" SELECT time, price FROM godzilla where time > current_timestamp - interval 1 month"""
        sparkSession.sql(monthQuery)
      case _ => sparkSession.sql("SELECT time, price FROM godzilla where false")
    }

    import sparkSession.implicits._
    val res: Dataset[Price] = result.sort(desc("time"))
      .map(r => Price(r.getTimestamp(0), r.getString(1)))

    val rowJosn = (if (rollingavg > 1) {
      import services.MovingAverageFunction._
      clearQueue
      implicit val movingAvgPeriod = rollingavg
      res.map(r => r match {
        case Price(time: Timestamp, price: String) => Price(time, movingAvg(price.toDouble).toString)
      })
    } else res)
      .toJSON.collect()

    Ok(rowJosn.mkString("[", ",", "]"))
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

    val qry = if (a && b) {
      s""" SELECT time , price FROM godzilla where time >= "$start" and time <= "$end" """
    }
    else
      s"""SELECT time, price FROM godzilla where false"""
    import sparkSession.implicits._
    val res: Dataset[Price] = sparkSession.sql(qry).sort(desc("time")).map(r => Price(r.getTimestamp(0), r.getString(1)))

    val rowJosn = (if (rollingAvg > 1) {
      import services.MovingAverageFunction._
      clearQueue
      implicit val movingAvgPeriod = rollingAvg
      res.map(r => r match {
        case Price(time: Timestamp, price: String) => Price(time, movingAvg(price.toDouble).toString)
      })
    } else res)
      .toJSON.collect()
    //    Ok(s"start:$start end:$end rolling:$rollingAvg")
    Ok(rowJosn.mkString("[", ",", "]"))
  }

  def priceForecast(days: Int, rollingAvg: Int) = Action {
    val sparkSession = Init.getSparkSessionInstance
    val doubVals = sparkSession.sql("select price from godzilla").sort(asc("time")).rdd.map(r => r.getString(0)).map(_.toDouble)

    val ts = Vectors.dense {
      if (rollingAvg > 1) {
        import services.MovingAverageFunction._
        clearQueue
        implicit val movingAvgPeriod = rollingAvg
        doubVals.collect.map(movingAvg)
      }
      else
        doubVals.collect
    }

    val arimaModel = ARIMA.fitModel(1, 1, 1, ts)
    //    println("coefficients: " + arimaModel.coefficients.mkString(","))
    //    println(ts.toArray.mkString(","))
    val forecast = arimaModel.forecast(ts, days).toArray.drop(ts.size) //toArray.drop(ts.size)

    //    Ok(s" pedicted days $days with rolling$rollingAvg")
    Ok(s" Future pedicted $days days price using ARIMA method of time series analysis ${forecast.mkString("[", ",", "]")}")
  }

}



