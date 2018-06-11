package bootstrap

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api._

object Init extends GlobalSettings {

  var sparkSession: SparkSession = _
  var priceTimeDF: DataFrame = _

  /**
    * On start load the json data from conf/data.json into in-memory Spark
    */
  override def onStart(app: Application) {
    sparkSession = SparkSession.builder
      .master("local")
      .appName("Bitcoin data backend system")
      .getOrCreate()


    val dataFrame = sparkSession.read.json("conf/data")
    priceTimeDF = dataFrame.withColumn("prices", explode(col("data.prices")))
      //      .select("prices.price", "prices.time")
      .select(col("prices.price") as "price", col("prices.time").cast("timestamp")).distinct()
    priceTimeDF.createOrReplaceTempView("godzilla")
  }

  /**
    * On stop clear the sparksession
    */
  override def onStop(app: Application) {
    sparkSession.stop()
  }

  def getSparkSessionInstance = {
    sparkSession
  }

  def getPriceTimeDF = priceTimeDF
}


