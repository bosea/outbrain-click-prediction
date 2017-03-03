package ml.dolphin.kaggle.outbrain

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Reads individual datasets, creates a sampled dataset based on display_id, and joins the files on appropriate
  * columns to generate the training and validation datasets.
  * Outputs are written to HDFS.
  *
  * Created by Abhijit Bose on 11/6/16.
  */
object ModelInputPipeline {

  def main(args: Array[String]): Unit = {

    // Turn off lots of debugging
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.WARN)

    // Set paths to input data, checkpoint directory and sampled data
    System.out.println(args)
    val inputPath = args(0)
    val sampledPath = args(1)
    val modelInputPath = args(2)
    val checkpointPath = args(3)
    val sampleRatio = args(4).toDouble

    // Set Spark configurations
    val conf = new SparkConf().setAppName("Ultimate Click Prediction Ensemble - Training Data Prep Pipeline")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)
    import sqlc.implicits     // This is required for rdd to df conversions
    sc.setCheckpointDir(checkpointPath)

    // Check if previously computed sampled datafiles exist. In that case, do not sample but proceed to joining
    // the sampled datasets. Otherwise, sample the original files into a smaller set of files.
    val fs = FileSystem.get(new Configuration())
    val rawDatafiles = fs.listStatus(new Path(sampledPath))
    if (rawDatafiles.isEmpty) {
      FeatureExtraction.sampleOriginalFiles(sc, sqlc, inputPath, sampledPath, sampleRatio)
    }

    // Create modeling dataset and save to HDFS
    val trainingPath = modelInputPath + "/training_rows_sample_" + sampleRatio + ".csv"
    FeatureExtraction.createModelingDatasetM1(sc, sqlc, sampledPath, sampleRatio)
  }
}
