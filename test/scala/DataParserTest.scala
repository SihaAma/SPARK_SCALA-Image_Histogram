import org.apache.spark.sql.functions.{col, typedLit}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions.{size,col}

class DataParserTest extends AnyFunSuite with SparkSessionTestWrapper with DataFrameTestUtils {

  import spark.implicits._

  val imagepath = "snow.jpg"
  val bucket = 5

  test("DataFrame Schema Test") {


    val image_df = spark.read.format("image").load(imagepath)
    image_df.printSchema()
    val cvModel: HistogramTransformModel = new HistogramTransform()
      .setInputCol("image")
      .setOutputCol("histogram")
      .setBucket(bucket)
      .fit(image_df)
    val resDf=cvModel.transform(image_df)

    resDf.printSchema()

    val intarray=Array[Int](bucket)

    val expectedDf=image_df.withColumn("histogram",typedLit(intarray))
    expectedDf.printSchema()

    assert(assertSchema(resDf.schema, expectedDf.schema))
  }

  test("DataFrame Data Test") {
    val image_df = spark.read.format("image").load(imagepath)
    image_df.printSchema()
    val cvModel: HistogramTransformModel = new HistogramTransform()
      .setInputCol("image")
      .setOutputCol("histogram")
      .setBucket(bucket)
      .fit(image_df)
    val resDf=cvModel.transform(image_df)
    resDf.select(col("histogram")).show(false)

    val intarray=image_df.select(col("image.data"))
      .rdd.flatMap(f => f.getAs[Array[Byte]]("data")).histogram(bucket)._2.array.map(_.toInt)

    val expectedDf=image_df.withColumn("histogram",typedLit(intarray))
    expectedDf.select(col("histogram")).show(false)

    assert(assertData(resDf, expectedDf))
  }
}
