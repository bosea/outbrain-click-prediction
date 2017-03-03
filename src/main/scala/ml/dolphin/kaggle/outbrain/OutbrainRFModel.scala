import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, BinaryClassificationEvaluator}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.feature.{IndexToString, VectorAssembler, StringIndexer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Fit a random forest classifier to Outbrain training data
  *
  * @author Abhijit Bose
  * @version 1.0 01/21/2017
  * @since 1.0 01/21/2017
  */

object OutbrainRFModel {

  def main(args: Array[String]): Unit = {

    // Turn off lots of debugging
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("Model Generation for Outbrain Click Prediction")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)
    import sqlc.implicits._

    // Arguments for various model generation parameters
    val sampledDataPath = args(0)
    val trainDataPath = args(1)
    val testDataPath = args(2)
    val checkpointPath = args(3)
    val numTrees = args(4).toInt
    val maxDepth = args(5).toInt
    val maxBins = args(6).toInt
    val numFolds = args(7).toInt
    val tuneParams = args(8).toBoolean

    // Following are hyperparameter tuning options, entered in args as "number,number"
    // format. For example, min and max of numTrees is "10,2000"  as a string in CLI argument.
    // quick-and-dirty. Please never do this in production code!

    var numTreesOpts = Array(0, 0)
    var maxDepthOpts = Array(0, 0)
    var maxBinsOpts = Array(0, 0)
    if (tuneParams) {
      numTreesOpts = args(9).split(",").map(_.toInt)
      maxDepthOpts = args(10).split(",").map(_.toInt)
      maxBinsOpts = args(11).split(",").map(_.toInt)
    }
    sc.setCheckpointDir(checkpointPath)

    // read the sampled down dataset
    /*
    val rows0 = sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "0.0")
      .option("delimiter", ",")
      .option("treatEmptyValuesAsNulls", "true")
      .load(sampledDataPath + "/sklearn_df_saved.csv")

    // add metadata to columns
    val columns = scala.collection.mutable.ListBuffer.empty[StructField]
    rows0.dtypes.foreach(s => {
      columns += StructField(s._1, DoubleType, true, Metadata.empty)
    })
    val newSchema = StructType(columns.toList)
    val rows = sqlc.createDataFrame(rows0.rdd, newSchema)
    */

    val rows = sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load(sampledDataPath + "/sklearn_df_saved.csv")
    rows.show(5)

    // all columns except for first column are included as features
    val allFeatures = scala.collection.mutable.ListBuffer[String]()
    val dblFeatures = scala.collection.mutable.ListBuffer[String]()
    val intFeatures = scala.collection.mutable.ListBuffer[String]()

    val listIntVars = List("ad_document_id", "ad_id", "campaign_id", "advertiser_id",
      "source_id", "publisher_id", "display_id", "platform",
      "geo1", "geo2", "geo3")
    val listDblVars = List("label", "topic_id", "category_id", "dayofweek", "hours")

    rows.dtypes.foreach(s => {
      listDblVars.foreach(x => {
        if (s._1.contains(x)) {
          dblFeatures += s._1
        }})
      listIntVars.foreach(x => {
        if (s._1.contains(x)) {
          intFeatures += s._1
        }})
    })
    allFeatures ++= dblFeatures ++ intFeatures
    rows.dtypes.foreach(println)
    allFeatures.foreach(s => println("allFeatures: " + s))

    //val rowsFinal = rows.na.fill(0.0, dblFeatures.toSeq).na.fill(0, intFeatures.toSeq)

    // Checkpoint the RDD so we don't climb up the lineage graph again!
    // This will save us from overgrowing the stack during feature indexing.
    //println("rows = " + rowsFinal.count())
    //rowsFinal.show(5)
    //rowsFinal.cache()

    // Split into training and validation datasets
    //rows.cache()
    val Array(trainDF, testDF) = rows.randomSplit(Array(0.7, 0.3))
    val t = rows.sample(false, 0.7)

    // Checkpoint training and validation datasets as well.

    println("Training Data = " + trainDF.count())
    println("Test Data = " + testDF.count())

    t.show(5)
    trainDF.show(5)
    testDF.show(5)

    // Stages for the pipeline for all the tasks, from feature engineering, model training and validation.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .fit(trainDF)

    // Array of features to include in the model
    val assembler = new VectorAssembler().setInputCols(allFeatures.toArray).setOutputCol("features")

    val rf = new RandomForestClassifier()
      .setLabelCol("labelIndex")
      .setFeaturesCol("features")
      .setNumTrees(numTrees)
      .setMaxDepth(30)

    val pipeline = new Pipeline().setStages(Array(labelIndexer, assembler, rf))

    //val mydf = pipeline.fit(trainDF).transform(trainDF)

    //mydf.show(5)
    //val myfeat = mydf.select("labelIndex", "features").show(5)

    // Build the model by fitting training data to the pipeline stages
    val model = pipeline.fit(trainDF)
    val treeModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    treeModel.toDebugString

    // Test the model with validation data
    val predictions = model.transform(testDF)
    predictions.filter(predictions("label") === 1.0).select("label", "labelIndex", "prediction", "probability").show(10)

    // Evaluate model TBD
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("labelIndex")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val areaUnderROC = evaluator.evaluate(predictions)
    println("Area Under ROC = " + areaUnderROC)
  }
}

