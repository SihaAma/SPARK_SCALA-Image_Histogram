import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, typedLit, udf}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

//trait SimpleIndexerParams extends Params {
// final val inputCol= new Param[String](this, "inputCol", "The input column")
// final val outputCol = new Param[String](this, "outputCol", "The output column")
//}


class HistogramTransform(override val uid: String) extends Estimator[HistogramTransformModel]with HasInputCol with HasOutputCol  {


  val bucket: Param[Int] = new Param(this, "bucket", """bucket for histogram""")
  setDefault(bucket, 10)

  def this() = this(Identifiable.randomUID("HistogramTransform"))

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setBucket(value: Int): this.type = set(bucket, value)


  override def copy(extra: ParamMap): HistogramTransform = {
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

  override def fit(dataset: Dataset[_]): HistogramTransformModel = {
    import dataset.sparkSession.implicits._

    //val histogram_array = img_array.zipWithIndex().map { f =>(f._1 & 0xFF)
    //}.histogram(4)._2.array.map(_.toInt)

    val histogram_array = dataset.select(col("image.data"))
      .rdd.flatMap(f => f.getAs[Array[Byte]]("data")).histogram($(bucket))._2.array.map(_.toInt)

    new HistogramTransformModel(uid, histogram_array)
    ; }
}


class HistogramTransformModel(override val uid: String,histogram_array: Array[Int]) extends Model[HistogramTransformModel] with HasInputCol with HasOutputCol {
  //def this() = this(Identifiable.randomUID("ImageToHistogramModel"))

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setInputCol(value: String): this.type = set(inputCol, value)

  override def copy(extra: ParamMap): HistogramTransformModel = {
    defaultCopy(extra)
  }


  override def transformSchema(schema: StructType): StructType = {

    // Add the return field
    schema.add(StructField($(outputCol), IntegerType, false))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.withColumn("histogram",typedLit(histogram_array))
  }


}