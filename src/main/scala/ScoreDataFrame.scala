////////////////////////////////////////////////////////////////////////////////////////////
// Functions to score a Spark DataFrame using an H2O POJO model.
////////////////////////////////////////////////////////////////////////////////////////////

import _root_.hex.genmodel.GenModel
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import scala.collection.immutable.IndexedSeq
import org.apache.spark.sql.types._

object ScoreDataFrame {
  /**
   * Function to arrange dataframe columns in the order that the POJO model expects.
   * Also, add and fills any missing columns with NULLs (currently uses sql).
   * colsToKeep are appended. Used for target column and index column if needed.
   * @param model a POJO model (compiled using GenModel)
   * @param inputDF an sql DataFrame to be scored
   * @param colsToKeep an array of column names from df to be kept in the output (eg key, target columns)
   * @param sqlContext requires the sqlContext to be specified
   * @return a DataFrame ready for scoring
   */
  def organiseDF(model: GenModel,
                 inputDF : DataFrame,
                 colsToKeep : Array[String],
                 sqlContext : SQLContext): DataFrame = {

    inputDF.registerTempTable("inputDF")
    val missingCols = model.getNames.toList.diff(inputDF.columns.toList)
    val appendCols = if (colsToKeep.length < 1) "" else {
      ", "+colsToKeep.toList.toString.substring(5,colsToKeep.toList.toString.length-1)
    }
    val inputDF0 = if (missingCols == List()) inputDF else {
      println("Warning: Missing features from dataset. Replacing with Nulls.")
      val missingColsQuery0 = for(i <- missingCols.indices) yield {
        "NULL as "+"`"+missingCols(i)+"`"
      }
      val missingColsQuery = "select *, "+missingColsQuery0.toString.substring(7,missingColsQuery0.toString.length-1)+" from inputDF"
      sqlContext.sql(missingColsQuery)
    }
    inputDF0.registerTempTable("inputDF0")
    val modelNames = for (c <- model.getNames) yield "`"+c+"`"
    val modelColsString = modelNames.toList.toString.substring(5,modelNames.toList.toString.length-1)
    sqlContext.sql("select "+modelColsString+appendCols+" from inputDF0")
  }

  /**
   * Score DataFrame with loaded POJO model. Outputs RDD of Rows.
   * @param model a POJO model (compiled using GenModel)
   * @param df an sql DataFrame to be scored
   * @param colsToKeep an array of column names from df to be kept in the output (eg key, target columns)
   * @return RDD[Row] of scored data
   */
  def scoreDFWithPojo(model: GenModel,
                      df: DataFrame,
                      colsToKeep: Array[String]): RDD[Row] = {
    val domainValues = model.getDomainValues
    /* Convert each feature of each row into a doubles */
    val output = df.map(r => {
      val rRecoded: IndexedSeq[Double] = for (i <- 0 to model.getNumCols - 1) yield
      if (model.getDomainValues.apply(i) != null && r(i) != null) {
        model.mapEnum(model.getColIdx(model.getNames.apply(i)), r(i).toString).toDouble
      }
      else if (model.getDomainValues.apply(i) != null && r(i) == null) -1.0
      else r(i) match {
          case i1: Int => i1.toDouble
          case b: Byte => b.toDouble
          case l: Long => l.toDouble
          case f: Float => f.toDouble
          case d: Double => d
          case _ => Double.NaN
        }
      /* Columns to keep to be appended */
      val appendSeq = if (colsToKeep.length < 1) Seq() else {
          for (i <- colsToKeep.indices) yield r(model.getNumCols + i)
        }
      Row.fromSeq(appendSeq ++
        model.score0(rRecoded.toArray, new Array[Double](model.getNumResponseClasses + 1)).toSeq
        )
    })
    output
  }

  /**
   * Takes original Dataframe and scored RDD and outputs a typed DataFrame
   * @param model a POJO model (compiled using GenModel)
   * @param origData Dataframe before scoriing
   * @param scoredData RDD[Row] of scored data
   * @param colsToKeep columns from origData to be kept
   * @return scored Dataframe
   */
  def scoredToDataframe(model: GenModel,
                        origData: DataFrame,
                        scoredData: RDD[Row],
                        colsToKeep: Array[String]): DataFrame = {
    val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
    val colsToKeepSchema = for (c <- colsToKeep) yield origData.schema(origData.columns.indexWhere(_== c))
    val predSchema = for (i <- 0 to model.getNumResponseClasses) yield
      if (i == 0) StructField("Pred",DoubleType,true) else StructField("P"++(i-1).toString,DoubleType,true)
    val Schema = new StructType(colsToKeepSchema ++ predSchema)
    sqlContext.createDataFrame(scoredData,Schema)
  }

  /**
   * Brings together organiseDF, scoreDFWithPojo and scoredToDataframe.
   * @param model a POJO model (compiled using GenModel)
   * @param df an sql DataFrame to be scored
   * @param colsToKeep an array of column names from df to be kept in the output (eg key, target columns)
   * @return an sql dataframe with model scores and colsToKeep
   */
  def organiseScoreOutput(model: GenModel,
                          df: DataFrame,
                          colsToKeep: Array[String],
                          sqlConext : SQLContext): DataFrame = {
    val orgDF = organiseDF(model,df,colsToKeep,sqlConext)
    val output0 = scoreDFWithPojo(model,orgDF,colsToKeep)
    scoredToDataframe(model,orgDF,output0,colsToKeep)
  }
}