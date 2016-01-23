////////////////////////////////////////////////////////////////////////////////////////////
// Function to score a Spark Dataframe (more precisely an RDD[Row]) using an H2O POJO model.
////////////////////////////////////////////////////////////////////////////////////////////

import _root_.hex.genmodel.GenModel
import org.apache.spark.sql._
import scala.collection.immutable.IndexedSeq

object ScoreDataFrame {
  /**
   * Score sql df with loaded pojo model.
   * If responseAttached = true then response must be final column of df
   * and of String type (at this stage only implemented for classification).
   */
  def scoreDFWithPojo(model: _root_.hex.genmodel.GenModel,
                      df: org.apache.spark.rdd.RDD[Row],
                      responseAttached: Boolean): org.apache.spark.rdd.RDD[Array[Double]] = {
    val domainValues = model.getDomainValues
    /** Create hash map to map the response column (if it exists) to Double */
    val hashMap = scala.collection.mutable.HashMap.empty[String, Int]
    if (responseAttached) {
      for (j <- 0 to domainValues(domainValues.length - 1).length - 1) {
        hashMap += domainValues(domainValues.length - 1)(j) -> j
      }
    }
    // Convert each row into a row of Doubles
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

      /** run model on encoded rows. If responseAttached = true output response as the last column as Double. */
      if (responseAttached) {
        model.score0(rRecoded.toArray, new Array[Double](model.getNumResponseClasses + 1)) ++
          Array(hashMap(r.getString(r.length - 1)).toDouble)
      }
      else model.score0(rRecoded.toArray, new Array[Double](model.getNumResponseClasses + 1))
    })
    output
  }
}