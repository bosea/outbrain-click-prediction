package ml.dolphin.kaggle.outbrain

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Abhijit Bose (boseae@gmail.com) on 2/20/17.
  */

object SplitModelingDataset {

  def main(args: Array[String]): Unit = {

    // Turn off lots of debugging
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.WARN)

    val sampledPath = args(0)
    val modelInputPath = args(1)
    val ratio = args(2)
    val splitRatio = args(3).toDouble

    val conf = new SparkConf().setAppName("DataFrame Join debug pipeline")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    val clicks_train = sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls","true")
      .option("nullValue","0.0")
      .load(sampledPath + "/clicks_train_join_" + ratio + ".csv")
      .withColumnRenamed("ad_id", "c_ad_id")
      .withColumnRenamed("display_id", "c_display_id")
    clicks_train.registerTempTable("ClicksTable")

    val promos = sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls","true")
      .option("nullValue","0.0")
      .load(sampledPath + "/promos_join_" + ratio + ".csv")
    promos.registerTempTable("PromoTable")

    val events = sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls","true")
      .option("nullValue","0.0")
      .load(sampledPath + "/events_join_" + ratio + ".csv")
    events.registerTempTable("EventsTable")

    /*
    clicks_train.dtypes.foreach(println)
    println("_________________________________________________________________")
    promos.dtypes.foreach(println)
    println("_________________________________________________________________")
    events.dtypes.foreach(println)
    println("_________________________________________________________________")
    */
    val query = "SELECT * " +
      "FROM ClicksTable C " +
      "LEFT JOIN PromoTable P ON C.c_ad_id = P.ad_id " +
      "LEFT JOIN EventsTable E on C.c_display_id = E.display_id"

    val rows = sqlc.sql(query)

    val derived = scala.collection.mutable.ListBuffer[String]()
    val listVars = List("category_id", "entity_id", "topic_id", "hours", "label")
    rows.dtypes.foreach(s => listVars.foreach(x => {
      if (s._1.contains(x)) {
        derived += s._1
      }})
    )

    //println(derived.toString())

    val rowsFinal = rows.na.fill(0.0, derived.toSeq)
    rowsFinal.dtypes.foreach(println)
    rowsFinal.show(10)


    rowsFinal.write.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls","true")
      .option("nullValue","0.0")
      .save(modelInputPath + "/model_input_sampled_" + ratio + ".csv")

    val Array(trainDF, testDF) = rowsFinal.randomSplit(Array(splitRatio, (1.0 - splitRatio)))

    trainDF.write.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls","true")
      .option("nullValue","0.0")
      .save(modelInputPath + "/train_" + ratio + ".csv")

    testDF.write.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls","true")
      .option("nullValue","0.0")
      .save(modelInputPath + "/test_" + ratio + ".csv")

  }
}
