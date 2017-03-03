package ml.dolphin.kaggle.outbrain

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{IndexToString, VectorAssembler, OneHotEncoder, StringIndexer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Methods to fit different models with the Outbrain training data
  *
  * @author Abhijit Bose
  * @version 1.0 01/21/2017
  * @since 1.0 01/21/2017
  */

object ModelGeneration {

  def main(args: Array[String]): Unit = {

    // Turn off lots of debugging
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("Model Generation for Outbrain Click Prediction")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

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

    var numTreesOpts = Array(0, 0)
    var maxDepthOpts = Array(0, 0)
    var maxBinsOpts = Array(0, 0)
    if (tuneParams) {
      numTreesOpts = args(9).split(",").map(_.toInt)
      maxDepthOpts = args(10).split(",").map(_.toInt)
      maxBinsOpts = args(11).split(",").map(_.toInt)
    }
    sc.setCheckpointDir(checkpointPath)

    // read training and test data sets

    /*
    val trainData = sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("nullValue", "0.0")
      .load(trainDataPath)

    val testData = sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("nullValue", "0.0")
      .load(testDataPath)
    */
    val sampledData = sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "0.0")
      .option("delimiter", ",")
      .option("treatEmptyValuesAsNulls", "true")
      .load(sampledDataPath + "/model_input_sampled_0.001.csv")

    //sampledData.dtypes.foreach(println)
    //sampledData.show(5)

    val derived = scala.collection.mutable.ListBuffer[String]()
    val listVars = List("category_id", "entity_id", "topic_id", "hours", "label")
    sampledData.dtypes.foreach(s => listVars.foreach(x => {
      if (s._1.contains(x)) {
        derived += s._1
      }})
    )

    //println(derived.toString())

    val rowsFinal = sampledData.na.fill(0.0, derived.toSeq).na.drop()
    //rowsFinal.dtypes.foreach(println)
    rowsFinal.show(10)

    rowsFinal.rdd.checkpoint()
    println("rowsFinal = " + rowsFinal.count())

    sampledData.unpersist()

    val Array(trainDF, testDF) = rowsFinal.randomSplit(Array(0.7, 0.3))

    trainDF.na.drop()

    trainDF.rdd.checkpoint()
    testDF.rdd.checkpoint()
    println("Training Data = " + trainDF.count())
    println("Test Data = " + testDF.count())

    trainDF.show(10)
    testDF.show(10)

    // Stages for the pipeline for all the tasks, from feature engineering, model training and validation.
    val stages = new ArrayBuffer[PipelineStage]()
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .setHandleInvalid("skip")
      .fit(rowsFinal)

    // checkpoint dataframes to avoid running into stack overflow later in ml pipeline
    //trainDF.dtypes.foreach(println)

    //trainDF.rdd.checkpoint()
    //testDF.rdd.checkpoint()

    //trainDF.count()
    //testDF.count()

    //val cpTrainDF = sqlc.createDataFrame(trainDF.rdd, trainDF.schema)
    //val cpTestDF = sqlc.createDataFrame(testDF.rdd, testDF.schema)

    // index some of the columns
    // encode and index the features as appropriate

    val cDisplayIdIndexer = new StringIndexer().setInputCol("c_display_id")
      .setOutputCol("c_display_id_index").fit(rowsFinal)
    val adDocumentIdIndexer = new StringIndexer().setInputCol("ad_document_id")
      .setOutputCol("ad_document_id_index").fit(rowsFinal)
    val cAdIdIndexer = new StringIndexer().setInputCol("c_ad_id")
      .setOutputCol("c_ad_id_index").fit(rowsFinal)
    val advertiserIdIndexer = new StringIndexer().setInputCol("advertiser_id").setOutputCol("advertiser_id_index")
      .fit(rowsFinal)
    val campaignIdIndexer = new StringIndexer().setInputCol("campaign_id").setOutputCol("campaign_id_index")
      .fit(rowsFinal)
    val geo1Indexer = new StringIndexer().setInputCol("geo1").setOutputCol("geo1_index")
      .fit(rowsFinal)
    val geo2Indexer = new StringIndexer().setInputCol("geo2").setOutputCol("geo2_index")
      .fit(rowsFinal)
    val geo3Indexer = new StringIndexer().setInputCol("geo3").setOutputCol("geo3_index")
      .fit(rowsFinal)
    val onDocumentIdIndexer = new StringIndexer().setInputCol("on_document_id").setOutputCol("on_document_id_index")
      .fit(rowsFinal)
    val publisherIdIndexer = new StringIndexer().setInputCol("publisher_id").setOutputCol("publisher_id_index")
      .fit(rowsFinal)
    val platformIndexer = new StringIndexer().setInputCol("platform").setOutputCol("platform_index")
      .fit(rowsFinal)
    val sourceIdIndexer = new StringIndexer().setInputCol("source_id").setOutputCol("source_id_index")
      .fit(rowsFinal)

    val cDisplayIdEncoder = new OneHotEncoder().setInputCol("c_display_id_index").setOutputCol("c_display_id_vec")
    val adDocumentIdEncoder = new OneHotEncoder().setInputCol("ad_document_id_index").setOutputCol("ad_document_id_vec")
    val cAdIdEncoder = new OneHotEncoder().setInputCol("c_ad_id_index").setOutputCol("c_ad_id_vec")
    val advertiserIdEncoder = new OneHotEncoder().setInputCol("advertiser_id_index").setOutputCol("advertiser_id_vec")
    val campaignIdEncoder = new OneHotEncoder().setInputCol("campaign_id_index").setOutputCol("campaign_id_vec")
    val dayofweekEncoder = new OneHotEncoder().setInputCol("dayofweek").setOutputCol("dayofweek_vec")
    val geo1Encoder = new OneHotEncoder().setInputCol("geo1_index").setOutputCol("geo1_vec")
    val geo2Encoder = new OneHotEncoder().setInputCol("geo2_index").setOutputCol("geo2_vec")
    val geo3Encoder = new OneHotEncoder().setInputCol("geo3_index").setOutputCol("geo3_vec")
    val onDocumentIdEncoder = new OneHotEncoder().setInputCol("on_document_id_index").setOutputCol("on_document_id_vec")
    val publisherIdEncoder = new OneHotEncoder().setInputCol("publisher_id_index").setOutputCol("publisher_id_vec")
    val platformEncoder = new OneHotEncoder().setInputCol("platform_index").setOutputCol("platform_vec")
    val sourceIdEncoder = new OneHotEncoder().setInputCol("source_id_index").setOutputCol("source_id_vec")

    val featuresArray = derived.toArray ++ Array(
      "c_display_id_vec",
      "c_ad_id",
      //"ad_document_id_vec",
      "advertiser_id_vec",
      "campaign_id_vec",
      "dayofweek_vec",
      "geo1_vec",
      "geo2_vec",
      "geo3_vec",
      "hours",
      //"on_document_id_vec",
      "platform_vec",
      "publisher_id_vec",
      "source_id_vec"
    )

    val assembler = new VectorAssembler().setInputCols(featuresArray).setOutputCol("features")

    val rf = new RandomForestClassifier()
      .setLabelCol("labelIndex")
      .setFeaturesCol("features")
      .setNumTrees(numTrees)
      .setMaxDepth(10)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    stages += (
      labelIndexer, cDisplayIdIndexer,
      //adDocumentIdIndexer, cAdIdIndexer,
      advertiserIdIndexer, campaignIdIndexer,
      geo1Indexer, geo2Indexer, geo3Indexer,
      //onDocumentIdIndexer,
      platformIndexer, publisherIdIndexer, sourceIdIndexer,
      cDisplayIdEncoder,
      //adDocumentIdEncoder, cAdIdEncoder,
      advertiserIdEncoder, campaignIdEncoder,  dayofweekEncoder,
      geo1Encoder, geo2Encoder, geo3Encoder,
      //onDocumentIdEncoder,
      platformEncoder, publisherIdEncoder, sourceIdEncoder,
      assembler, rf, labelConverter)

    val pipeline = new Pipeline().setStages(stages.toArray)

    val model = pipeline.fit(trainDF)

    // Test the model with validation data
    val predictions = model.transform(testDF)
    predictions.filter(predictions("label") === 1.0).select("label", "labelIndex", "prediction", "predictedLabel").show(10)

    // Evaluate model
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("labelIndex")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC")
    val areaUnderROC = evaluator.evaluate(predictions)
    println("Area Under ROC = " + areaUnderROC)
  }
}
