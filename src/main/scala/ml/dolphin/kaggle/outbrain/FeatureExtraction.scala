package ml.dolphin.kaggle.outbrain

import java.util.Date

import breeze.numerics.{abs, pow}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.{VectorAssembler, OneHotEncoder, StringIndexer}
import org.apache.spark.sql.types._
import org.apache.spark.{sql, SparkContext}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}

import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.ListMap

/**
  * Various methods for extracting features from the Outbrain dataset
  *
  * @author Abhijit Bose
  * @version 1.0 01/21/2017
  * @since 1.0 01/21/2017
*/
object FeatureExtraction {

  // define a column transformation udf for handling missing value in a string column
  def udfMissingValue = sql.functions.udf((s: String) => if (s == null || s.isEmpty || s == " ") "9999999" else s)

  // entity_id's in document_entities.csv have large hex values, e.g. "f9eec25663db4cd83183f5c805186f16"
  val hashSize = pow(2, 24)
  def reduceEntityIdSize(entity: String) = abs(entity.hashCode % hashSize).toString
  val udfReduceEntityIdSize = sql.functions.udf(reduceEntityIdSize(_: String))

  // use index for a given column's category level to convert to string
  // val udfMapToIndex = sql.functions.udf( (key: String, mapF: Map[String, Int]) => mapF.getOrElse(key, 99999999).toDouble)

  // Extract country, state and other tokens out of geo_location field
  def splitGeoLocation(geo: String): Seq[String] = {
    if (geo == null || geo.isEmpty) {
      Seq("empty", "empty", "empty")
    } else {
      val tokens = geo.split(">")
      tokens.length match {
        case 3 => Seq(tokens(0), tokens(1), tokens(2))
        case 2 => Seq(tokens(0), tokens(1), "empty")
        case 1 => Seq(tokens(0), "empty", "empty")
        case _ => Seq("empty", "empty", "empty")
      }
    }
  }

  def splitTimestamp(ts: Long): Seq[Double] = {
    val date = new Date(1465876799998L + ts)
    Seq(date.getDay.toDouble, date.getHours.toDouble)
  }

  def splitEventsDFColumns(df: DataFrame, sqlc: SQLContext): DataFrame = {
    val rows = df.select("display_id", "document_id", "platform", "geo_location", "timestamp")
        .rdd.map(r => Row.fromSeq(
      Seq(r(0), r(1), r(2)) ++ splitGeoLocation(r.getString(3)) ++ splitTimestamp(r(4).toString.toLong)
    ))
    sqlc.createDataFrame(rows, OutbrainDataPreparer.eventsExtendedSchema)
  }

  /**
    * Transpose a DataFrame with repeated row keys, a category column and a frequency/score column
    * into another DataFrame in which first column has only unique row keys and subsequent columns
    * contain the category column levels along with the corresponding frequency/score for each level
    * as a Double value. For example,
    *
    * The row keys "d1", "d2" and "d3" are repeated in multiple rows. If the row keys are not repeated, i.e.
    * each row has a single unique key, we can take advantage of Spark ML library's in-built
    * 1-hot encoding functions.
    *
    * +-------------+----------+--------+
    * |document_id  | topic_id | value  |
    * +-------------+----------+--------+
    * |  d1         |  t1      |   0.2  |
    * |  d1         |  t2      |   0.1  |
    * |  d2         |  t4      |   0.3  |
    * |  d1         |  t3      |   0.4  |
    * |  d3         |  t4      |   5.0  |
    * |  d3         |  t1      |   7.0  |
    * +-------------+----------+--------+
    *
    * transposedColumnEncoder returns the following DataFrame as output by aggregating
    * all topic_id's for a document_id, and then creating encoded columns with the corresponding
    * values from value column:
    *
    * +-------------+-------------+-------------+-------------+-------------+
    * |document_id  | topic_id_t1 | topic_id_t2 | topic_id_t3 | topic_id_t4 |
    * +-------------+-------------+-------------+-------------+-------------+
    * |  d1         |    0.2      |     0.1     |      0.4    |      0.0    |
    * |  d2         |    0.0      |     0.0     |      0.0    |      0.3    |
    * |  d3         |    7.0      |     0.0     |      0.0    |      5.0    |
    * +-------------+-------------+-------------+-------------+-------------+
    *
    * @param df Input DataFrame
    * @param keyColumn column to be used as primary key in output DataFrame
    * @param encColumn column to flatten and encode
    * @param valColumn column for the values
    * @param sc SparkContext
    * @param sqlc SQLContext created before calling the method
    * @return DataFrame with column transposed and unique values
    */
  def transposedColumnEncoder(df: DataFrame, keyColumn: String, encColumn: String, valColumn: String,
                              sc: SparkContext, sqlc: SQLContext): DataFrame = {

    // Get distinct levels of df(encColumn) entries, required to create column names for a new schema.
    val levelsMap = df.select(encColumn).distinct.map(_.getString(0)).collect.sorted.zipWithIndex.toMap
    val newColumnNames = levelsMap.keys.map(encColumn + "_" + _).toArray.sorted
    val columns = scala.collection.mutable.ListBuffer.empty[StructField]
    columns += StructField(keyColumn, StringType, true)
    newColumnNames.foreach(columns += StructField(_, DoubleType, true))
    val newSchema = StructType(columns.toList)

    // Aggregate (encColumn, valColumn) by distinct keyColumn values.
    val bcLevelsMap = sc.broadcast(levelsMap)
    val kvMaps = df.select(keyColumn, encColumn, valColumn).rdd.map(r =>
        (r(0).toString -> List((r(1).toString, r(2))))).reduceByKey(_ ::: _).cache()

    // generate a new DataFrame from kvMaps and columns
    val rows = kvMaps.map(r => {
      val row = scala.collection.mutable.Seq.fill(levelsMap.size){0.0}
      r._2.foreach(x => row(bcLevelsMap.value(x._1)) = x._2.asInstanceOf[Double])
      Row.fromSeq(Seq(r._1) ++ row)
    })
    sqlc.createDataFrame(rows, newSchema)
  }

  /*
     val rdd = sc.parallelize(List(
       ("d1", "t1", 0.2),
       ("d1", "t2", 0.1),
       ("d2", "t4", 0.3),
       ("d1", "t3", 0.4),
       ("d3", "t4", 5.0),
       ("d3", "t1", 7.0)))
     val df = sqlContext.createDataFrame(rdd).toDF("document_id", "topic_id", "value")
     val transposed = transposedColumnEncoder(df, "document_id", "topic_id", "value", sc, sqlContext)
     transposed.show()
     +-----------+-----------+-----------+-----------+-----------+
     |document_id|topic_id_t1|topic_id_t2|topic_id_t3|topic_id_t4|
     +-----------+-----------+-----------+-----------+-----------+
     |         d1|        0.2|        0.1|        0.4|        0.0|
     |         d2|        0.0|        0.0|        0.0|        0.3|
     |         d3|        7.0|        0.0|        0.0|        5.0|
     +-----------+-----------+-----------+-----------+-----------+

   val rdd = sc.parallelize(List(
    ("1172584","8fb694b25a41a6ffbb37870d7b15b9ef",0.683064045820828),
    ("1420697","8a8db986b90776780bb4c98e12edc662",0.524362183814848),
    ("1420697","04d8f9a1ad48f126d5806a9236872604",0.278713754102544),
    ("850547","6ad3979c1cf1fe576c6cf4e1025c1e3b",0.959992425449086),
    ("1065906","404185dbfa887e2d1e2ee3f469aef9b5",0.726948764917303),
    ("1065906","ca39b3c67defc11cd14e90891429d87c",0.221430635053449),
    ("1065906","7a57603aa958fee295b278d0b018abf1",0.218148555720101),
    ("2618443","17d848ee1e0c763555cff2cdd58d13db",0.912160466016287),
    ("2618443","b592850a51791d19823b82262339f661",0.238622248790703),
    ("2618443","c49d667f69eb211f5cbe2ab5006d24cf",0.214578836528577),
    ("2094718","94101adfc2f6bccba21ccd48fcd0efe2",0.315181755164833),
    ("2094718","024d2d0130c4b1737554ad62ba94c6fa",0.261373589416613),
    ("1009082","7fcb37dfd9d59d7259fb03e19e94bc21",0.322696903675233),
    ("1009082","7696177d85f3c8aeef6b1ee5cfca0e20",0.321951433839185),
    ("1009082","51aba117d83f71b49e112cf9315b487c",0.24070640382304),
    ("1009082","ada8047fb13822507a936898620ae896",0.235384917904931),
    ("1009082","0ae34ea1dd1d16fc733951ae80a29b64",0.228228934295196),
    ("1009082","3abb604f2fc6eec417bc014f45ceec1f",0.219600421349235),
    ("1009082","9383b596eb330de86cf8057e647f2320",0.218639764181016),
    ("1009082","1f5ab5e74ee5251f36c7a8374d0b4e87",0.217097443804758),
    ("1009082","93bfd084cda436d72637d990087b7737",0.215068524086341),
    ("1009082","9196d1673fb0cc89f1790d4be2604419",0.209758635652331),
    ("757798","b5a19d632a95dd242d42580267d4b596",0.188555789345085),
    ("757798","82546670421112f0369eca0dc23f3842",0.182408060384279)))
    val df = sqlContext.createDataFrame(rdd).toDF("document_id", "entity_id", "confidence_level")
    val transposed = transposedColumnEncoder(df, "document_id", "entity_id", "confidence_level", sc, sqlContext)
    transposed.show()
  */

  def createModelingDatasetM1(sc: SparkContext, sqlc: SQLContext, sampledPath: String, ratio: Double) = {

    // Read all the sampled down files and create dataframes. Assumption is that the sampled down
    // version of the problem fits in memory allocated to the Spark job.  All sampled dataframes
    // has an added "_s" to their names indicated these are samples.

    val clicks_train_s = OutbrainDataPreparer.createDataFrameWithSchema(sqlc, sampledPath +
      "/clicks_train_sample_" + ratio + ".csv", ",", OutbrainDataPreparer.clicksTrainSchema)
      .withColumnRenamed("clicked", "label")

    val events_s = OutbrainDataPreparer.createDataFrameWithSchema(sqlc, sampledPath +
      "/events_sample_" + ratio + ".csv", ",", OutbrainDataPreparer.eventsSchema)
    val events = splitEventsDFColumns(events_s, sqlc).withColumnRenamed("document_id", "on_document_id")

    //val ads_docs = events_features.select("document_id").distinct().map(_.getString(0)).collect().toSet

    val promoted_s = OutbrainDataPreparer.createDataFrameWithSchema(sqlc, sampledPath +
      "/promoted_content_sample_" + ratio + ".csv", ",", OutbrainDataPreparer.promotedContentSchema)

    //val advertisers_unq = sc.broadcast(promoted_s.select("advertiser_id").distinct().map(_.getString(0)).collqect()
    //  .zipWithIndex.toMap)
    //val campaigns_unq = sc.broadcast(promoted_s.select("campaign_id").distinct().map(_.getString(0)).collect()
    //  .zipWithIndex.toMap)

    //val promo_docs = promoted_s.select("document_id").distinct().map(_.getString(0)).collect().toSet
    //val doc_ids_bc = sc.broadcast(ads_docs.union(promo_docs).toList)

    val categories_s = OutbrainDataPreparer.createDataFrameWithSchema(sqlc, sampledPath +
      "/document_categories_sample_" + ratio + ".csv", ",", OutbrainDataPreparer.documentsCategoriesSchema).distinct()
    val topics_s = OutbrainDataPreparer.createDataFrameWithSchema(sqlc, sampledPath +
      "/document_topics_sample_" + ratio + ".csv", ",", OutbrainDataPreparer.documentsTopicsSchema)
    val meta_s = OutbrainDataPreparer.createDataFrameWithSchema(sqlc, sampledPath +
      "/documents_meta_sample_" + ratio + ".csv", ",", OutbrainDataPreparer.documentsMetaSchema)
    val entities_s = OutbrainDataPreparer.createDataFrameWithSchema(sqlc, sampledPath +
      "/document_entities_sample_" + ratio + ".csv", ",", OutbrainDataPreparer.documentsEntitiesSchema)

    val categories = categories_s.filter(categories_s("confidence_level") >= 0.7)
      .withColumnRenamed("document_id", "ad_document_id")
    val categories_f = transposedColumnEncoder(categories, "ad_document_id", "category_id", "confidence_level",
      sc, sqlc).drop("confidence_level")

    val topics = topics_s.filter(topics_s("confidence_level") >= 0.7)
      .withColumnRenamed("document_id", "ad_document_id")
    val topics_f = transposedColumnEncoder(topics, "ad_document_id", "topic_id", "confidence_level", sc, sqlc)
      .drop("confidence_level")

    val entities = entities_s.filter(entities_s("confidence_level") >= 0.95)
      .withColumnRenamed("document_id", "ad_document_id")
    val entities_f = transposedColumnEncoder(entities, "ad_document_id", "entity_id", "confidence_level", sc, sqlc)
      .drop("confidence_level")

    val metas = meta_s.withColumnRenamed("document_id", "ad_document_id")
      .withColumn("publisher_id", udfMissingValue(meta_s("publisher_id")))
      .withColumn("source_id", udfMissingValue(meta_s("source_id")))
      .drop("publish_time")

    // Rename document_id columns in events and promoted_content since they represent different meanings
    // use index for a given column's category level to convert to double

    val promos = promoted_s.withColumnRenamed("document_id", "ad_document_id")
      .withColumn("campaign_id", udfMissingValue(promoted_s("campaign_id")))
      .withColumn("advertiser_id", udfMissingValue(promoted_s("advertiser_id")))

    // get all document related metadata for each document_id in a single dataframe
    val docs = topics_f
      .join(categories_f, Seq("ad_document_id"), "outer")
      .join(entities_f, Seq("ad_document_id"), "outer")
      .na.fill(0.0)

    val metas_docs = metas.join(docs, Seq("ad_document_id"), "leftouter")
    val promos_f = promos.join(metas_docs, Seq("ad_document_id"), "leftouter").na.fill("9999999", Seq("ad_id"))

    clicks_train_s.write.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls","true")
      .option("nullValue", "0.0")
      .save(sampledPath + "/clicks_train_join_" + ratio + ".csv")

    promos_f.write.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls","true")
      .option("nullValue","0.0")
      .save(sampledPath + "/promos_join_" + ratio + ".csv")

    events.write.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls","true")
      .option("nullValue","0.0")
      .save(sampledPath + "/events_join_" + ratio + ".csv")

  }


  /**
    * Process the sampled down files to generate training and test datasets.
    *
    * @param sc
    * @param sqlc
    * @param sampledPath
    * @param ratio
    */
  def createModelingDatasetM2(sc: SparkContext, sqlc: SQLContext, sampledPath: String, ratio: Double): DataFrame = {

    // Read all the sampled down files and create dataframes. Assumption is that the sampled down
    // version of the problem fits in memory allocated to the Spark job.  All sampled dataframes
    // has an added "_s" to their names indicated these are samples.

    val clicks_train_s = OutbrainDataPreparer.createDataFrameWithSchema(sqlc, sampledPath +
      "/clicks_train_sample_" + ratio + ".csv", ",", OutbrainDataPreparer.clicksTrainSchema)
    val events_s = OutbrainDataPreparer.createDataFrameWithSchema(sqlc, sampledPath +
      "/events_sample_" + ratio + ".csv", ",", OutbrainDataPreparer.eventsSchema)
    val categories_s = OutbrainDataPreparer.createDataFrameWithSchema(sqlc, sampledPath +
      "/document_categories_sample_" + ratio + ".csv", ",", OutbrainDataPreparer.documentsCategoriesSchema)
    val topics_s = OutbrainDataPreparer.createDataFrameWithSchema(sqlc, sampledPath +
      "/document_topics_sample_" + ratio + ".csv", ",", OutbrainDataPreparer.documentsTopicsSchema)
    val meta_s = OutbrainDataPreparer.createDataFrameWithSchema(sqlc, sampledPath +
      "/documents_meta_sample_" + ratio + ".csv", ",", OutbrainDataPreparer.documentsMetaSchema)
    val promoted_s = OutbrainDataPreparer.createDataFrameWithSchema(sqlc, sampledPath +
      "/promoted_content_sample_" + ratio + ".csv", ",", OutbrainDataPreparer.promotedContentSchema)
    val entities_s = OutbrainDataPreparer.createDataFrameWithSchema(sqlc, sampledPath +
      "/document_entities_sample_" + ratio + ".csv", ",", OutbrainDataPreparer.documentsEntitiesSchema)

    // define a column transformation udf for converting string to double and missing value
    def toDoubleFixRecord(element: String) = {
      try (element.toDouble) catch { case _ => 999999.0 }
    }
    val udfDoubleColumn = sql.functions.udf(toDoubleFixRecord(_: String))



    // define a missing column udf for strings. Insert "Missing" as the column value.
    def missingStringFixRecord(element: String) = element match {
      case null => "Missing"
      case "" => "Missing"
      case _ => element
    }
    val udfMissingString = sql.functions.udf(missingStringFixRecord(_: String))

    // Process events_s and add a few features



    // Join all the datasets and select the final set of feature columns
    val displays = clicks_train_s.withColumn("label", clicks_train_s("clicked"))
      .join(events_s.withColumn("platform", udfDoubleColumn(events_s("platform")))
        .withColumn("geo_location", udfMissingString(events_s("geo_location"))), "display_id")

    /*
      events_s.withColumn("platform", udfDoubleColumn(events_s("platform")))
      .join(clicks_train_s.withColumn("label", clicks_train_s("clicked")), "display_id")
     */

    val docs = meta_s.withColumn("publisher_id", udfMissingValue(meta_s("publisher_id")))
      .withColumn("source_id", udfMissingString(meta_s("source_id")))
      .join(promoted_s, Seq("document_id"))
      .join(categories_s.withColumn("category_id", udfMissingValue(categories_s("category_id"))), "document_id")
      .join(topics_s.withColumn("topic_id", udfMissingValue(topics_s("topic_id"))), "document_id")


    /*
    topics_s.withColumn("topic_id", udfMissingStringColumn(topics_s("topic_id")))
    .join(categories_s.withColumn("category_id", udfMissingStringColumn(categories_s("category_id"))), "document_id")
    .join(meta_s.withColumn("publisher_id", udfMissingStringColumn(meta_s("publisher_id")))
                .withColumn("source_id", udfMissingString(meta_s("source_id"))),
          Seq("document_id"), "right_outer")
    .join(promoted_s, Seq("document_id"), "right_outer")
    */

    val rows = displays.join(docs, "ad_id")
      .select("label",
        "document_id",
        "ad_id",
        "display_id",
        "platform",
        "geo_location",
        "source_id",
        "category_id",
        "topic_id",
        "publisher_id")
    //  .na.fill(9999999.0, Seq("category_id", "topic_id", "source_id"))
    rows
  }

  /**
    * Sample the original datasets to generate a set of smaller datasets and save them
    * on disk.
    *
    * @param sc
    * @param sqlc
    * @param trainPath
    * @param sampledPath
    * @param ratio
    * @return
    */
  def sampleOriginalFiles(sc: SparkContext, sqlc: SQLContext, trainPath: String,
                          sampledPath: String, ratio: Double) = {

    // Read the click training data set (clicks_train.csv) and generate the list of unique display_id's.
    // Sample the display_id's and filter training data to generate a sampled-down dataset.

    val clicks_train_df = OutbrainDataPreparer.createDataFrameWithSchema(
      sqlc, trainPath + "/clicks_train.csv", ",", OutbrainDataPreparer.clicksTrainSchema)
    val clicks_train_sample = clicks_train_df.sample(false, ratio).cache()

    //val display_id_train = clicks_train_sample.select("display_id").distinct()
    //val display_id_sample_unq = display_id_train.sample(false, ratio)

    val display_id_sample_unq = clicks_train_sample.select("display_id").distinct()
    val display_id_bcast = sc.broadcast(display_id_sample_unq.map(_.getString(0)).collect())

    val ad_id_sample_unq = clicks_train_sample.select("ad_id").distinct().cache()
    val ad_id_bcast = sc.broadcast(ad_id_sample_unq.map(_.getString(0)).collect())

    //val clicks_train_sample = clicks_train_df.filter(clicks_train_df("display_id").isin(display_id_bcast.value:_*))
    //  .cache()


    println("training sample length = " + clicks_train_sample.count())
    clicks_train_sample.write.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .save(sampledPath + "/clicks_train_sample_" + ratio + ".csv")
    ad_id_sample_unq.write.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .save(sampledPath + "/ad_id_sample_" + ratio + ".csv")
    display_id_sample_unq.write.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .save(sampledPath + "/display_id_sample_" + ratio + ".csv")

    // Read events.csv and filter it by display_id_bcast.value entries. Build a table of
    // display_id, platform, geo_location

    val events_df = OutbrainDataPreparer.createDataFrameWithSchema(
      sqlc, trainPath + "/events.csv", ",", OutbrainDataPreparer.eventsSchema)
    val events_sample = events_df.filter(events_df("display_id").isin(display_id_bcast.value:_*))
    events_sample.write.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .save(sampledPath + "/events_sample_" + ratio + ".csv")

    // Read promoted_content.csv file and filter based on ad_id_bcast values
    val promoted_content_df = OutbrainDataPreparer.createDataFrameWithSchema(
      sqlc, trainPath + "/promoted_content.csv", ",", OutbrainDataPreparer.promotedContentSchema)
    val promoted_content_adid_sample = promoted_content_df
      .filter(promoted_content_df("ad_id").isin(ad_id_bcast.value:_*))
    val document_id_sample = promoted_content_adid_sample.select("document_id").distinct().cache()
    val document_id_bcast = sc.broadcast(document_id_sample.map(_.getString(0)).collect())

    promoted_content_adid_sample.write.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .save(sampledPath + "/promoted_content_sample_" + ratio + ".csv")
    document_id_sample.write.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .save(sampledPath + "/document_id_sample_" + ratio + ".csv")

    // Read document_topics.csv and filter based on document_id_bcast values
    val doc_topics_df = OutbrainDataPreparer.createDataFrameWithSchema(
      sqlc, trainPath + "/documents_topics.csv", ",", OutbrainDataPreparer.documentsTopicsSchema)
    val doc_topics_sample = doc_topics_df.filter(doc_topics_df("document_id").isin(document_id_bcast.value:_*))
    doc_topics_sample.write.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .save(sampledPath + "/document_topics_sample_" + ratio + ".csv")

    // Read document_categories.csv, and filter by document_id_bcast values
    val doc_cats_df = OutbrainDataPreparer.createDataFrameWithSchema(
      sqlc, trainPath + "/documents_categories.csv",
      ",", OutbrainDataPreparer.documentsCategoriesSchema)
    val doc_cats_sample = doc_cats_df.filter(doc_cats_df("document_id").isin(document_id_bcast.value:_*))
    doc_cats_sample.write.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .save(sampledPath + "/document_categories_sample_" + ratio + ".csv")

    // Read document entities.csv: filter out entities with document_id's
    val doc_entities_df = OutbrainDataPreparer.createDataFrameWithSchema(
      sqlc, trainPath + "/documents_entities.csv",
      ",", OutbrainDataPreparer.documentsEntitiesSchema)
    val doc_entities_sample = doc_entities_df.filter(doc_entities_df("document_id").isin(document_id_bcast.value:_*))
    doc_entities_sample.write.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .save(sampledPath + "/document_entities_sample_" + ratio + ".csv")

    // Read document_meta.csv and filter by document_id_bcast values
    val doc_meta_df = OutbrainDataPreparer.createDataFrameWithSchema(
      sqlc, trainPath + "/documents_meta.csv", ",", OutbrainDataPreparer.documentsMetaSchema)
    val doc_meta_sample = doc_meta_df.filter(doc_meta_df("document_id").isin(document_id_bcast.value:_*))
    doc_meta_sample.write.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .save(sampledPath + "/documents_meta_sample_" + ratio + ".csv")
  }

  // Generate features from the modeling dataset
  /**
    *
    * @param df
    * @return
    */
  def generateFeatures(df: DataFrame): ArrayBuffer[PipelineStage] = {

    val stages = new ArrayBuffer[PipelineStage]()

    // encode and index the features as appropriate

    val adDocumentIdIndexer = new StringIndexer().setInputCol("ad_document_id")
      .setOutputCol("ad_document_id_index").fit(df)
    val adIdIndexer = new StringIndexer().setInputCol("ad_id")
      .setOutputCol("ad_id_index").fit(df)

    val advertiserIdIndexer = new StringIndexer().setInputCol("advertiser_id").setOutputCol("advertiser_id_index")
      .fit(df)
    val campaignIdIndexer = new StringIndexer().setInputCol("campaign_id").setOutputCol("campaign_id_index")
      .fit(df)
    //val categoryIdIndexer = new StringIndexer().setInputCol("category_id").setOutputCol("category_id_index")
    //  .fit(df)
    val displayIdIndexer = new StringIndexer().setInputCol("display_id").setOutputCol("display_id_index")
      .fit(df)
    //val entityIdIndexer = new StringIndexer().setInputCol("entity_id").setOutputCol("entity_id_index")
    //  .fit(df)
    val geo1Indexer = new StringIndexer().setInputCol("geo1").setOutputCol("geo1_index")
      .fit(df)
    val geo2Indexer = new StringIndexer().setInputCol("geo2").setOutputCol("geo2_index")
      .fit(df)
    val geo3Indexer = new StringIndexer().setInputCol("geo3").setOutputCol("geo3_index")
      .fit(df)
    val onDocumentIdIndexer = new StringIndexer().setInputCol("on_document_id").setOutputCol("on_document_id_index")
      .fit(df)
    val publisherIdIndexer = new StringIndexer().setInputCol("publisher_id").setOutputCol("publisher_id_index")
      .fit(df)
    val platformIndexer = new StringIndexer().setInputCol("platform").setOutputCol("platform_index")
      .fit(df)
    val sourceIdIndexer = new StringIndexer().setInputCol("source_id").setOutputCol("source_id_index")
      .fit(df)
    //val topicIdIndexer = new StringIndexer().setInputCol("topic_id").setOutputCol("topic_id_index")
    //  .fit(df)


    val advertisers = df.select("advertiser_id").distinct().map(_.getString(0)).collect()
    advertisers.foreach(s => println("stages = " + s))

    val adDocumentIdEncoder = new OneHotEncoder().setInputCol("ad_document_id_index").setOutputCol("ad_document_id_vec")
    val adIdEncoder = new OneHotEncoder().setInputCol("ad_id_index").setOutputCol("ad_id_vec")
    val advertiserIdEncoder = new OneHotEncoder().setInputCol("advertiser_id_index").setOutputCol("advertiser_id_vec")
    val campaignIdEncoder = new OneHotEncoder().setInputCol("campaign_id_index").setOutputCol("campaign_id_vec")
    //val categoryIdEncoder = new OneHotEncoder().setInputCol("category_id_index").setOutputCol("category_id_vec")
    val dayofweekEncoder = new OneHotEncoder().setInputCol("dayofweek").setOutputCol("dayofweek_vec")
    val displayIdEncoder = new OneHotEncoder().setInputCol("display_id_index").setOutputCol("display_id_vec")
    //val entityIdEncoder = new OneHotEncoder().setInputCol("entity_id_index").setOutputCol("entity_id_vec")
    val geo1Encoder = new OneHotEncoder().setInputCol("geo1_index").setOutputCol("geo1_vec")
    val geo2Encoder = new OneHotEncoder().setInputCol("geo2_index").setOutputCol("geo2_vec")
    val geo3Encoder = new OneHotEncoder().setInputCol("geo3_index").setOutputCol("geo3_vec")
    val onDocumentIdEncoder = new OneHotEncoder().setInputCol("on_document_id_index").setOutputCol("on_document_id_vec")
    val publisherIdEncoder = new OneHotEncoder().setInputCol("publisher_id_index").setOutputCol("publisher_id_vec")
    val platformEncoder = new OneHotEncoder().setInputCol("platform_index").setOutputCol("platform_vec")
    val sourceIdEncoder = new OneHotEncoder().setInputCol("source_id_index").setOutputCol("source_id_vec")
    //val topicIdEncoder = new OneHotEncoder().setInputCol("topic_id_index").setOutputCol("topic_id_vec")

    /*
    val featuresVector = Array(
      "ad_document_id_vec",
      "ad_id_vec",
      "advertiser_id_vec",
      "campaign_id_vec",
      "category_id_vec",
      "dayofweek_vec",
      "display_id_vec",
      "entity_id_vec",
      "geo1_vec",
      "geo2_vec",
      "geo3_vec",
      "hours",
      "on_document_id_vec",
      "platform_vec",
      "publisher_id_vec",
      "source_id_vec",
      "topic_id_vec"
    )

  val featuresVector = Array(
    "advertiser_id_vec",
    "campaign_id_vec",
    "category_id_vec",
    "display_id_vec",
    "geo1_vec",
    "geo2_vec",
    "geo3_vec",
    "platform_vec",
    "publisher_id_vec",
    "source_id_vec",
    "topic_id_vec"
  )
  */

    val derived = scala.collection.mutable.ListBuffer[String]()
    val listVars = List("category_id", "entity_id", "topic_id")
    df.dtypes.foreach(s => listVars.foreach(x => {
      if (s._1.contains(x)) {
        derived += s._1
      }})
    )

    val featuresArray = derived.toArray ++ Array(
      "campaign_id_vec",
      "display_id_vec",
      "geo1_vec",
      "geo2_vec",
      "geo3_vec",
      "platform_vec",
      "publisher_id_vec",
      "source_id_vec",
      "hours",
      "dayofweek_vec"
    )

    val assembler = new VectorAssembler().setInputCols(featuresArray).setOutputCol("features")


    stages += (
      advertiserIdIndexer, campaignIdIndexer, displayIdIndexer, geo1Indexer,
      geo2Indexer, geo3Indexer, platformIndexer, publisherIdIndexer, sourceIdIndexer,
      dayofweekEncoder, advertiserIdEncoder, campaignIdEncoder,  displayIdEncoder,
      geo1Encoder, geo2Encoder, geo3Encoder, platformEncoder, publisherIdEncoder, sourceIdEncoder,
      assembler)

    /*
    stages += (
     adDocumentIdIndexer, adIdIndexer, advertiserIdIndexer, campaignIdIndexer, categoryIdIndexer,
      displayIdIndexer, entityIdIndexer,  geo1Indexer, geo2Indexer, geo3Indexer, onDocumentIdIndexer,
      publisherIdIndexer, platformIndexer, sourceIdIndexer,
      topicIdIndexer,  adDocumentIdEncoder, adIdEncoder,
      advertiserIdEncoder, campaignIdEncoder, categoryIdEncoder, dayofweekEncoder, displayIdEncoder, entityIdEncoder,
      geo1Encoder, geo2Encoder, geo3Encoder, onDocumentIdEncoder,
      publisherIdEncoder, platformEncoder, sourceIdEncoder, topicIdEncoder, assembler
      )
      */

    //stages.foreach(p => println(p.explainParams()))

    //val pipeline = new Pipeline().setStages(stages.toArray)
    //val dftrain1 = pipeline.fit(df)
    //val dftrain = dftrain1.transform(df)
    //dftrain.show(2)
    //dftrain.dtypes.foreach(println)

    //val featuresFinal = dftrain.select("features")
    //featuresFinal.dtypes.foreach(s => println(s))
    //val c = featuresFinal.toJSON
    //println(c.first())

    stages
  }

}
