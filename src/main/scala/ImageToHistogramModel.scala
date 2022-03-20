import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

//trait SimpleIndexerParams extends Params {
 // final val inputCol= new Param[String](this, "inputCol", "The input column")
 // final val outputCol = new Param[String](this, "outputCol", "The output column")
//}


class ImageToHistogram(override val uid: String) extends Estimator[ImageToHistogramModel]with HasInputCol with HasOutputCol  {

  def this() = this(Identifiable.randomUID("ImageToHistogram"))

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setInputCol(value: String): this.type = set(inputCol, value)



  override def copy(extra: ParamMap): ImageToHistogram = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    // Check that the input type is a string
    val idx = schema.fieldIndex($(inputCol))
    val field = schema.fields(idx)
    if (field.dataType != StringType) {
      throw new Exception(s"Input type ${field.dataType} did not match input type StringType")
    }
    // Add the return field
    schema.add(StructField($(outputCol), IntegerType, false))
  }

  override def fit(dataset: Dataset[_]): ImageToHistogramModel = {
    import dataset.sparkSession.implicits._
    val words = dataset.select(dataset($(inputCol)).as[String]).distinct
      .collect()
    new ImageToHistogramModel(uid, words)
    ; }
}


class ImageToHistogramModel(override val uid: String,words: Array[String]) extends Model[ImageToHistogramModel] with HasInputCol with HasOutputCol {
  //def this() = this(Identifiable.randomUID("ImageToHistogramModel"))

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setInputCol(value: String): this.type = set(inputCol, value)

  override def copy(extra: ParamMap): ImageToHistogramModel = {
    defaultCopy(extra)
  }


  private val labelToIndex: Map[String, Double] = words
    .zipWithIndex.
    map{case (x, y) => (x, y.toDouble)}.toMap

  override def transformSchema(schema: StructType): StructType = {

    // Add the return field
    schema.add(StructField($(outputCol), IntegerType, false))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val indexer = udf { label: String => labelToIndex(label) }
    dataset.select(col("color"),
      indexer(dataset("color").cast(StringType)).as($(outputCol)))
  }


}










//class ImageToHistogram {}
