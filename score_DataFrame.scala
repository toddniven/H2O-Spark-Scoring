// Function to score a Spark Dataframe (more precisely an RDD[Row]) using an H2O POJO model.
// To load POJO model first compile and package POJO into jar (say pojo_model.jar), then include jar as part of SPARK_SUBMIT_OPTIONS
// e.g. export SPARK_SUBMIT_OPTIONS = "--jars /home/some.jar,/home/pojo_model.jar"
// Now load POJO model by reflection 'val model = Class.forName("pojo_model").newInstance().asInstanceOf[GenModel]'

import _root_.hex.genmodel.GenModel // Depends on H2O's Sparkling Water package
import org.apache.spark.sql._

// Score sql df with loaded pojo model.
// If responseAttached = true then response must be final column of df and of String type (at this stage only implemented for classification).
def scoreDFWithPojo(model : _root_.hex.genmodel.GenModel, df : org.apache.spark.rdd.RDD[Row], responseAttached : Boolean) : org.apache.spark.rdd.RDD[Array[Double]] = {
    val domainValues = model.getDomainValues
    // Create hash map to map the response column (if it exists) to Double
    val hashMap = scala.collection.mutable.HashMap.empty[String,Int]
    if(responseAttached){for (j <- 0 to domainValues(domainValues.length-1).length -1){hashMap+=domainValues(domainValues.length-1)(j)->j}}
    // Specify the record length
    val recordLength = domainValues.length - 2
    // Convert record into a record of Doubles
        val output = df.map( r => {
        val rRecoded = for (i <- 0 to recordLength) yield
            if (model.getDomainValues(i) != null && r(i) != null) model.mapEnum(model.getColIdx(model.getNames.apply(i)), r(i).toString).toDouble else 
            if (model.getDomainValues(i) != null && r(i) == null) -1.0 else
            if (r(i).isInstanceOf[Int]) r(i).asInstanceOf[Int].toDouble else
            if (r(i).isInstanceOf[Double]) r(i).asInstanceOf[Double] else
            0.0 // default to 0 if null is found in numeric column

        // run model on encoded rows. If responseAttached = true output response as the last column as Double.
        if (responseAttached) model.score0(rRecoded.toArray, new Array[Double](model.getNumResponseClasses+1)) ++ Array(hashMap(r.getString(r.length -1)).toDouble) else
        model.score0(rRecoded.toArray, new Array[Double](model.getNumResponseClasses+1))
        }
    )
    return output
}