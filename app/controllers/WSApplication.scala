package controllers

import java.sql.Timestamp

import bootstrap.Init
import org.apache.spark.sql.{Dataset, _}
import play.api.libs.functional.syntax._
import play.api.libs.json.{JsPath, JsValue, Json, Reads}
import play.api.mvc.{Action, Controller}
import services.Price

/**
  * Created by mshinde on 07-06-2018.
  */

class WSApplication extends Controller {
  def price(period: String, rollingavg:Int ) = Action{
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
      .toJSON.collect().mkString("[",",","]")

  val json: JsValue = Json.parse(rowJosn)

  implicit val priceReads: Reads[Point] = (
    (JsPath \\ "price").read[String] and
      (JsPath \\ "time").read[String]
    ) (Point.apply _)

  val rplyWith: Seq[Point] = (json).as[Seq[Point]]
  Ok(views.html.index(rplyWith))
}
}



