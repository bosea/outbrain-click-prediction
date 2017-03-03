package ml.dolphin.kaggle.outbrain

import org.apache.spark.SparkContext
import org.apache.spark.ml.{PipelineStage, Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{VectorAssembler, OneHotEncoder, StringIndexer}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql

import scala.collection.mutable.ArrayBuffer

object OutbrainDataPreparer {

  // Schemas for reading different data sets
  val pageViewsSchema = StructType(List(StructField("uuid", StringType, true),
    StructField("document_id", StringType, true),
    StructField("timestamp", StringType, true),
    StructField("platform", StringType, true),
    StructField("geo_location", StringType, true),
    StructField("traffic_source", IntegerType, true)
  ))

  val eventsSchema = StructType(List(StructField("display_id", StringType, true),
    StructField("uuid", StringType, true),
    StructField("document_id", StringType, true),
    StructField("timestamp", StringType, true),
    StructField("platform", StringType, true),
    StructField("geo_location", StringType, true)
  ))

  val eventsExtendedSchema = StructType(List(
    StructField("display_id", StringType, true),
    StructField("document_id", StringType, true),
    StructField("platform", StringType, true),
    StructField("geo1", StringType, true),
    StructField("geo2", StringType, true),
    StructField("geo3", StringType, true),
    StructField("dayofweek", DoubleType, true),
    StructField("hours", DoubleType, true)
  ))

  val promotedContentSchema = StructType(List(
    StructField("ad_id", StringType, true),
    StructField("document_id", StringType, true),
    StructField("campaign_id", StringType, true),
    StructField("advertiser_id", StringType, true)
  ))

  val clicksTrainSchema = StructType(List(
    StructField("display_id", StringType, true),
    StructField("ad_id", StringType, true),
    StructField("clicked", DoubleType, true)
  ))

  val documentsMetaSchema = StructType(List(StructField("document_id", StringType, true),
    StructField("source_id", StringType, true),
    StructField("publisher_id", StringType, true),
    StructField("publish_time", StringType, true)
  ))

  val documentsCategoriesSchema = StructType(List(
    StructField("document_id", StringType, true),
    StructField("category_id", StringType, true),
    StructField("confidence_level", DoubleType, true)
  ))

  val documentsTopicsSchema = StructType(List(
    StructField("document_id", StringType, true),
    StructField("topic_id", StringType, true),
    StructField("confidence_level", DoubleType, true)
  ))

  val documentsEntitiesSchema = StructType(List(
    StructField("document_id", StringType, true),
    StructField("entity_id", StringType, true),
    StructField("confidence_level", DoubleType, true)
  ))

  val predictionsAndLabelsSchema = StructType(List(
    StructField("prediction", DoubleType, true),
    StructField("labelIndex", DoubleType, true)
  ))

  val adIdSchema = StructType(List(StructField("ad_id", StringType, true)))
  val displayIdSchema = StructType(List(StructField("display_id", StringType, true)))
  val documentIdSchema = StructType(List(StructField("document_id", StringType, true)))

  /*
  val documentsCategoriesSampledSchema = StructType(List(StructField("document_id", StringType, true),
    StructField("category_id", StringType, true)))

  val documentsTopicsSampledSchema = StructType(List(StructField("document_id", StringType, true),
    StructField("topic_id", StringType, true)))

  val eventsSampledSchema = StructType(List(
    StructField("display_id", StringType, true),
    StructField("document_id", StringType, true),
    StructField("platform", StringType, true),
    StructField("geo1", StringType),
    StructField("geo2", StringType),
    StructField("geo3", StringType),
    StructField("dayofweek", IntegerType),
    StructField("hours", IntegerType)
  ))

  val promotedContentSampledSchema = StructType(List(StructField("ad_id", StringType, true),
    StructField("document_id", StringType, true)))
*/

  // Schema for final modeling dataset as saved - version M1
  val modelingDatasetSchemaM1 = StructType(List(
    StructField("label", DoubleType, true),
    StructField("ad_document_id", StringType, true),
    StructField("ad_id", StringType, true),
    StructField("advertiser_id", StringType, true),
    StructField("campaign_id", StringType, true),
    StructField("category_id", StringType, true),
    StructField("dayofweek", DoubleType, true),
    StructField("display_id", StringType, true),
    StructField("entity_id", StringType, true),
    StructField("geo1", StringType, true),
    StructField("geo2", StringType, true),
    StructField("geo3", StringType, true),
    StructField("hours", DoubleType, true),
    StructField("on_document_id", StringType, true),
    StructField("platform", StringType, true),
    StructField("publisher_id", StringType, true),
    StructField("source_id", StringType, true),
    StructField("topic_id", StringType, true)
  ))

  // Schema for final modeling dataset as saved - version M2
  val modelingDatasetSchemaM2 = StructType(List(
    StructField("label", DoubleType, true),
    StructField("document_id", StringType, true),
    StructField("ad_id", StringType, true),
    StructField("display_id", StringType, true),
    StructField("platform", DoubleType, true),
    StructField("geo_location", StringType, true),
    StructField("source_id", StringType, true),
    StructField("category_id", DoubleType, true),
    StructField("topic_id", DoubleType, true),
    StructField("publisher_id", StringType, true)
  ))

  // Schema for final modeling dataset as saved - version M1

  /**
    * Read input files and create dataframes - generic implementation
    */
  def createDataFrameWithSchema(sqlc: SQLContext, path: String, delim: String,
                                schema: StructType): DataFrame = {
    val df = sqlc.read.format("com.databricks.spark.csv")
      .option("delimiter", delim)
      .option("header", "true")
      .schema(schema)
      .load(path)
    return df
  }
}
