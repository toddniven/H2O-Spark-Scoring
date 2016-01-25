# H2O-Spark-Scoring
H2O machine learning models can be exported as POJO.
See https://github.com/h2oai/h2o-3/blob/master/h2o-docs/src/product/howto/POJO_QuickStart.md
Here we provide functions to score Spark data frames with H2O POJO models.
H2O POJO models take numeric arrays as input and output the prediction. Here we simply provide
a mapping from a dataframe row to an array and then apply the POJO to the array.

Step 1: Package the pojo model, in this case "model.java" (requires h2o-genmodel.jar):
```
javac -cp h2o-genmodel.jar -J-Xmx2g -J-XX:MaxPermSize=256m model.java
jar -cf model.jar *.class
```

Step 2: Include model.jar in Spark instance:
```
export SPARK_SUBMIT_OPTIONS="--driver-class-path .../sparkling-water-1.5.6/assembly/build/libs/sparkling-water-assembly-1.5.6-all.jar \
--jars .../sparkling-water-1.5.6/assembly/build/libs/sparkling-water-assembly-1.5.6-all.jar,.../model.jar
```

Step 3: In Spark, load model by reflection:
```scala
import _root_.hex.genmodel.GenModel
val model = Class.forName("model").newInstance().asInstanceOf[GenModel]
```

Step 4: Example usage:
```scala
val keepCols = Array("indexCol","targetCol")
val scoredDataFrame = ScoreDataFrame.organiseScoreOutput(model, df, keepCols, sqlContext)
scoredDataFrame.show
```
So far only tested on GBM classification models. 