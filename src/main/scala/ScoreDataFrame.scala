////////////////////////////////////////////////////////////////////////////////////////////
// Function to score a Spark Dataframe (more precisely an RDD[Row]) using an H2O POJO model.
////////////////////////////////////////////////////////////////////////////////////////////

import _root_.hex.genmodel.GenModel
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import scala.collection.immutable.IndexedSeq

object ScoreDataFrame {
  /**
   * Function to arrange dataframe columns in the order that the POJO model expects.
   * Also, add and fills any missing columns with NULLs (currently uses sql).
   * colsToKeep are appended. Used for target column and index column if needed.
   */
  def organiseDF(model: _root_.hex.genmodel.GenModel,
                 inputDF : org.apache.spark.sql.DataFrame,
                 colsToKeep : Array[String]) : org.apache.spark.sql.DataFrame = {
    val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
    inputDF.registerTempTable("inputDF")
    val missingCols = model.getNames.toList.diff(inputDF.columns.toList)
    val appendCols = if (colsToKeep.length < 1) "" else {
      ", "+colsToKeep.toList.toString.substring(5,colsToKeep.toList.toString.length-1)
    }
    val inputDF0 = if (missingCols == List()) inputDF else {
      println("Warning: Missing features from dataset. Replacing with Nulls.")
      val missingColsQuery0 = for(i <- missingCols.indices) yield {
        "NULL as "+missingCols(i)
      }
      val missingColsQuery = "select *, "+missingColsQuery0.toString.substring(7,missingColsQuery0.toString.length-1)+" from inputDF"
      sqlContext.sql(missingColsQuery)
    }
    inputDF0.registerTempTable("inputDF0")
    val modelColsString = model.getNames.toList.toString.substring(5,model.getNames.toList.toString.length-1)
    sqlContext.sql("select "+modelColsString+appendCols+" from inputDF0")
  }

  /**
   * Score sql df with loaded pojo model.
   * (TODO: add schema and output DF)
   */
  def scoreDFWithPojo(model: _root_.hex.genmodel.GenModel,
                      df0: org.apache.spark.sql.DataFrame,
                      colsToKeep: Array[String]): org.apache.spark.rdd.RDD[Row] = {
    val df = organiseDF(model,df0,colsToKeep) /* First arrange dataframe for scoring */
    val domainValues = model.getDomainValues
    /* Convert each feature of each row into a doubles */
    val output = df.map(r => {
      val rRecoded: IndexedSeq[Double] = for (i <- 0 to domainValues.length - 2) yield
      if (model.getDomainValues(i) != null && r(i) != null) {
        model.mapEnum(model.getColIdx(model.getNames.apply(i)), r(i).toString).toDouble
      }
      else
      if (model.getDomainValues(i) != null && r(i) == null) -1.0
      else
        r(i) match {
          case i1: Int => i1.toDouble
          case d: Double => d
          case _ => Double.NaN
        }
      /* Columns to keep to be appended */
      val appendSeq = if (colsToKeep.length < 1) Seq() else {
          for (i <- colsToKeep.indices) yield r(domainValues.length - 1 + i)
        }
      Row.fromSeq(model.score0(rRecoded.toArray, new Array[Double](model.getNumResponseClasses + 1)).toSeq
        ++ appendSeq)
    })
    output
  }
}