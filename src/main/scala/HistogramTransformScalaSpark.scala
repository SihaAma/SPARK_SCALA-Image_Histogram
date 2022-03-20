import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, countDistinct, udf}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.functions._



object HistogramTransformScalaSpark {

  def main(args: Array[String]): Unit = {
    val path = "car.jpg"
    val spark = SparkSession.builder().appName("imgSeg").master("local[*]").getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val image_df = spark.read.format("image").load(path)
    image_df.printSchema()

    val cvModel: HistogramTransformModel = new HistogramTransform()
      .setInputCol("image")
      .setOutputCol("histogram")
      .setBucket(5)
      .fit(image_df)


    val df4=cvModel.transform(image_df)
    val img_data = df4.select(col("histogram"))
    img_data.show(false)


    //val histoimage = new HistogramTransform()

    //histoimage.transform(image_df).show(false)





    //val img_array = image_df.select(col("image.data"))
    //  .rdd.flatMap(f => f.getAs[Array[Byte]]("data"))

    //val his=img_array.histogram(4)._2.array.map(_.toInt)

    //his.foreach{println}






    //val histoarray = Seq((his))

    //val df4 = image_df.withColumn("histogram",typedLit(his))

    //val df = histoarray.toDF("histo")
    //df4.select(col("image.histogram")).show()
    //df4.printSchema()

    //val img_data = df4.select(col("histogram"))
    //img_data.show()


    //print(img_data)





    import org.apache.spark.sql.functions._

    import spark.implicits._

    //val histo = img_array.zipWithIndex().map { f =>(f._1 & 0xFF)
    //}.histogram(4)._2.array.map(_.toInt)

    //histo.foreach{println}




/*

    var final_image_df = img_array.zipWithIndex().map { f =>(f._1 & 0xFF)
    }.toDF.withColumnRenamed("value", "color")
    final_image_df.show(false)



    val hist = final_image_df
      .select(col("color"))
      .rdd.map(r => r.getInt(0))
      .histogram(4)

    println(hist._1.toList)
    println(hist._2.toList)

    val cvModel: HistogramTransformModel = new HistogramTransform()
      .setInputCol("color")
      .setOutputCol("features")
      .fit(final_image_df)


    cvModel.transform(final_image_df).show(false)

    //hist.foreach{println}
    //val arr_contains_df = final_image_df.withColumn("result", array_contains($"array_col2", 3))

    //arr_contains_df.show()
    */



/*

    val indexer = udf { df: Dataset[_] => df
      .select(col("color"))
      .rdd.map(r => r.getInt(0))
      .histogram(4)._2.array.map(_.toInt) }

    val toto=final_image_df.select(col("*"),
      indexer(final_image_df("color")).as("hostogram"))

    toto.show(false)

*/



    /*

    val img_data = image_df.select(col("image.*"))
      .rdd.map(row => (
      row.getAs[Int]("height"),
      row.getAs[Int]("width"),
      row.getAs[Int]("nChannels"),
      row.getAs[Array[Byte]]("data")))
      .collect()(0)

    val height = img_data._1
    val width = img_data._2
    val nChannels = img_data._3

    var offSet = spark.sparkContext.longAccumulator("offSetAcc")
    var x = spark.sparkContext.longAccumulator("xAcc")
    var y = spark.sparkContext.longAccumulator("yAcc")
    x.add(1)
    y.add(1)

    import spark.implicits._
    var final_image_df = img_array.zipWithIndex().map { f =>

      if (offSet.value == 0) {
        offSet.add(1)
        if (f._2 != 0)
          x.add(1)
      } else if (offSet.value == 1) {
        //g
        offSet.add(1)
      } else if (offSet.value == 2) {
        //r
        offSet.reset()
      }
      if (x.value == (width)) {
        x.reset()
        y.add(1)
      }
      (f._1 & 0xFF)
    }.toDF.withColumnRenamed("value", "color")

    val image_double= final_image_df.withColumn("color",col("color").cast(DoubleType))

    val hist = image_double
      .select(col("color"))
      .rdd.map(r => r.getDouble(0))
      .histogram(4)._2.array.map(_.toInt)

    hist.foreach{println}

    //val arrays_int = udf((before:Seq[Double]) => before.map(_.toInt))

*/


    //val wordcount = udf { in: String => in.split(" ").size }
    //val pixcount = udf { in: String => in.split(" ").size }
    //image_double.select(col("color"),
    //  convertUDF(image_double.col("color")).as("color_counts")).show()

    /*

    val image_string= final_image_df.withColumn("color",col("color").cast(StringType))




    // fit a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("color")
      .setOutputCol("counts")
      .setVocabSize(256)
      .setMinDF(2)
      .fit(image_string)

    // alternatively, define CountVectorizerModel with a-priori vocabulary
    //val cvm = new CountVectorizerModel(Array("a", "b", "c"))
    //  .setInputCol("color")
    //  .setOutputCol("count")

    cvModel.transform(image_string).show(false)
*/




}


}

