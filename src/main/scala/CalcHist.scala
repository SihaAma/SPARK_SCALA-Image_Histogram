import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset, functions}
import org.apache.spark.sql.types.StructType

/*
class CalcHist extends Transformer with HasInputCol with HasOutputCol {

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setInputCol(value: String): this.type = set(inputCol, value)


  //def this() = this(Identifiable.randomUID("upp"))

  def transform(dataset: Dataset[_]): DataFrame = {
    var result = dataset.select(col("image.data"))
      .rdd.flatMap(f => f.getAs[Array[Byte]]("data")).map(r => r.toDouble).histogram(256)._2
    return result (${outputCol})
  }

  override def transform(image_df: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(image_df.schema, logging = true)
    val hist = image_df.select(col("image.data"))
      .rdd.flatMap(f => f.getAs[Array[Byte]]("data")).map(r => r.toDouble).histogram(256)

  }
  8/

}

*/


