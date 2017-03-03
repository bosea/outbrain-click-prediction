package ml.dolphin.kaggle.outbrain

import org.apache.spark.rdd.RDD


import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.{Row, SQLContext, DataFrame, Column}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd._


object ModelMatrix {

  /*
  case class Person(first: String, last: String, age: Int, state: String)
  case class MyTuple(age: Int, person: Person)

  val rdd = sc.parallelize(List(
      ("James", "Jones", 21, "CO"),
      ("Basil", "Brush" ,44, "AK")
    ))
  val df = sqlContext.createDataFrame(rdd).toDF("name", "alias", "age", "state")
  val df1 = modelMatrix(df)
  df1.show()
   */


  def apply(df: DataFrame, excludeList: List[String]): DataFrame = {
    modelMatrix(df, excludeList)
  }

  /**
    * modelMatrix takes an input dataframe, identifies the categorical columns based on
    * the column data type, and produces 1-hot encoding of the levels within each categorical
    * column. Any column that should not be processed in this manner (e.g. columns that will
    * not be used for model building pipeline) can be specified in an exclude list as defined
    * below.
    *
    * modelMatrix returns a new DataFrame with original and new columns defined.
    *
    * @param df Input DataFrame with a defined schema
    * @param excludeList A list of column names that should not be flattened
    * @return
    */
  def modelMatrix (df: DataFrame, excludeList: List[String]): DataFrame = {
    var catVars = ArrayBuffer[Column]()
    var otherVars = ArrayBuffer[Column]()
    popVarArrays(df, excludeList, catVars, otherVars)

    if (catVars.isEmpty) {
      castAll(df.select(otherVars:_*), DoubleType)
    } else {
      val varLevels = createLevelsMap(df, catVars)
      val newFields = otherVars ++ createDummies(varLevels)
      newFields.foreach(println)
      df.select(newFields:_*).foreach(println)
      castAll(df.select(newFields:_*), DoubleType)
    }
  }

  /**
    * Separate string and non-string columns except that are in exclude list,
    * and store them in separate Array[Column]
    *
    * @param df
    * @param excludeList
    * @param categorical
    * @param other
    */
  def popVarArrays(df: DataFrame,
                   excludeList: List[String],
                   categorical: ArrayBuffer[Column],
                   other: ArrayBuffer[Column]) = {

    df.dtypes.foreach{ elem =>
      if (elem._2 == "StringType" && !excludeList.contains(elem._1)) {
        categorical += df.apply(elem._1)
      } else {
        other += df.apply(elem._1)
      }
    }
  }

  //Map each column to an array of its unique values
  def createLevelsMap(df: DataFrame,
                      fields: ArrayBuffer[Column]): Map[Column, Array[String]] = {

    var levels: Map[Column, Array[String]] = Map()
    fields.foreach { field =>
      levels += (field -> getLevels(df, field))
    }
    levels
  }
  
  //Create an array of k-1 distinct categories for a single Column
  def getLevels(df: DataFrame, field: Column): Array[String] = {
    df.select(field).distinct.map(_.getString(0)).collect.sorted
    //slice(1, Integer.MAX_VALUE)
  }
  
  //Build an Array[Column] containing all of the new binary fields
  def createDummies(levelsMap: Map[Column, Array[String]]): Array[Column] = {
    levelsMap.keys.map { key =>
      explodeField(key, levelsMap(key))
    }.reduce { (acc, elem) =>
      acc ++ elem
    }
  }

  //Take a single Column and create an Array[Column] containing a binary field
  //for each distinct value
  def explodeField(field: Column, levels: Array[String]): Array[Column] = {
    levels.map { level =>
      when(field === level, 1).otherwise(0).as(f"$field%s_$level%s")
    }
  }

  /**
    * Convert a categorical column of a DataFrame with repeated row keys into 1-hot encoded
    * columns without any aggregation function. For example, given the following DataFrame:
    *
    * The row keys "u1" and "u2" are repeated in multiple rows. If the row keys are not repeated, i.e.
    * each row has a single unique key, we can take advantage of Spark ML library's in-built
    * 1-hot encoding functions.
    *
    * +----+----+--------+
    * |user|item|quantity|
    * +----+----+--------+
    * |  u1|  c1|       2|
    * |  u1|  c1|       5|
    * |  u2|  c2|       3|
    * |  u1|  c2|       4|
    * |  u1|  c2|     100|
    * +----+----+--------+
    *
    * groupedColumnOneHotEncoding returns the following DataFrame as output by aggregating
    * all items for a user, and then creating additional 1-hot encoded columns from the
    * items column:
    *
    * +-------+--------+----------+
    * | user | item_c1 | item_c2  |
    * +------+---------+----------+
    * |  u1  |    1    |    1     |
    * |  u2  |    0    |    1     |
    * +------+---------+----------+
    *
    * @param df Input DataFrame
    * @param keys array of column names to be used as a key into df
    * @param targetVar Categorical column that will be encoded as 1-hot columns
    * @param sqlc SQLContext created before calling this method
    * @return DataFrame with keys as columns and 1-hot encoded columns from targetVar
    */
  def groupedColumnOneHotEncoding(df: DataFrame,
                                  keys: List[String],
                                  targetVar: String,
                                  sqlc: SQLContext): DataFrame = {

    val levels = df.select(targetVar).distinct().map(_.getString(0)).collect()
    val levelsMap = levels.view.zipWithIndex.toMap
    val cols = List.concat(keys, List(targetVar))
    val newSchema = generateSchema(keys, targetVar + "_", levels)
    val temp = df.select(cols.head, cols.tail: _*).rdd.map(x => {
      x(0).asInstanceOf[String] -> List(x(1).asInstanceOf[String])
    }).reduceByKey((accum, next) => (next ::: accum).distinct)

    val rowRDD = temp.map(x => {
      var expanded = Array.fill(levels.length)(0.0)
      x._2.foreach(y => expanded(levelsMap(y)) = 1.0)
      Row.fromSeq(Seq(x._1) ++ expanded)
    })
    sqlc.createDataFrame(rowRDD, newSchema)
  }

  /**
    * Generates an on-demand schema for a Spark SQL table based on row keys and
    * column definitions.
    *
    * @param keys Primary keys of tabular data
    * @param prefix Common prefix to apply to new column names based on levels, including
    *               a separator (e.g. item_ or item# etc)
    * @param levels An array of values to generate the new column names
    * @return
    */
  def generateSchema(keys: List[String], prefix: String, levels: Array[String]): StructType = {
    var newSchema = Array[StructField]()
    keys.foreach(x => newSchema :+= StructField(x.asInstanceOf[String], StringType, true))
    levels.foreach(x => newSchema :+= StructField(prefix + x.asInstanceOf[String], DoubleType, true))
    StructType(newSchema)
  }

  /*
    if (cols.length > 4) {
      println("ERROR: groupedColumnOneHotEncoding can only handle a key of up to 3-tuples right now ")
      exit()
    }

    val temp = df.select(cols.head, cols.tail:_*).rdd.map(x => {
                   if (cols.length == 3)
                     (x(0).asInstanceOf[String], x(1).asInstanceOf[String]) -> List(x(2).asInstanceOf[String])
                   else if (cols.length == 4)
                     (x(0).asInstanceOf[String], x(1).asInstanceOf[String], x(2).asInstanceOf[String]) -> List(x(3).asInstanceOf[String])
                   else
                     x(0).asInstanceOf[String] -> List(x(1).asInstanceOf[String])})
                 .reduceByKey((accum, next) => (next ::: accum).distinct)
    val output = temp.map(x => {
                            var row = List()
                            var expanded = Array.fill(levels.length)(0.0)
                            if (cols.length == 3)
                              row = row ::: List(x._1.asInstanceOf[String], x._2.asInstanceOf[String])
                              expanded(levelsMap(x._3)) = 1.0
                            else if (cols.length == 4)
                              row = row ::: List(x._1, x._2, x._3)
                              expanded(levelsMap(x._4)) = 1.0
                            else
                              row = row ::: List(x._1)
                              expanded(levelsMap(x._2)) = 1.0
                            row = row ::: expanded.toList
                            row }).toDF(newCols)
  */


  //Cast all the fields of a DataFrame to a given DataType
  def castAll(df: DataFrame, to: DataType): DataFrame = {
    df.select( {
      df.columns.map { column =>
        df.apply(column).cast(to).as(column)
      }
    }:_* )
  }
}
