import Calc_histogram.{getHistogram, makeGray}
import org.apache.spark
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.col

import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO

object testtransform {
  def main(args: Array[String]): Unit = {
    val path = "car.jpg"
    val grey_scales = 256

    val imagePath = "path to image files"
    val spark = SparkSession.builder().appName("imgSeg").master("local[*]").getOrCreate()

    // Read image files as binary file
    val imageDF = spark.read.format("image").load(path)
    val img_array = imageDF.select(col("image.data"))
      .rdd.flatMap(f => f.getAs[Array[Byte]]("data"))

    import spark.implicits._
    var final_image_df = img_array.zipWithIndex().map { f =>(f._1 & 0xFF)
    }.toDF.withColumnRenamed("value", "color")
    final_image_df.show(false)





  }


  //val histogram = getHistogram(img_array,0)


    //val toto=df.select("image.origin", "image.width", "image.height").show(truncate=false)
    //print(toto)


    // Transform binary content to image
    /*val imagetohistogram = new HistogramTransform()
      .setInputCol("content")
      .setOutputCol("image")

    // Define Pipeline
    val pipeline = new Pipeline()
    pipeline.setStages(Array(
      imagetohistogram
    ))

    val modelPipeline = pipeline.fit(spark.emptyDataFrame)

    val data = modelPipeline.transform(df)

     */

    //data.show()


  //}
}

