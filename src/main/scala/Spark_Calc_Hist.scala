import java.awt.Color
import java.awt.color.ColorSpace
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO
import java.io.File
import scala.collection.JavaConverters._
import java.awt.image.BufferedImage
import org.apache.spark.annotation.Since
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import java.util.ArrayList
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.col
/*
object Spark_Calc_Hist {


  def pixels2gray(red: Int, green: Int, blue: Int): Int = (red + green + blue) / 3
  def makeGray(img: java.awt.image.BufferedImage): java.awt.image.BufferedImage = {
    val w = img.getWidth
    val h = img.getHeight
    for { w1 <- (0 until w).toVector
          h1 <- (0 until h).toVector
          } yield {
      val col = img.getRGB(w1, h1)
      val red =  (col & 0xff0000) / 65536
      val green = (col & 0xff00) / 256
      val blue = (col & 0xff)
      val graycol = pixels2gray(red, green, blue)
      img.setRGB(w1, h1, new Color(graycol, graycol, graycol).getRGB)
    }
    img
  }



  def getHistogram(image: BufferedImage): Array[Int]= {

    val numberOfBins=256

    val iw = image.getWidth
    val ih = image.getHeight
    val imageSize = ih * iw
    assert(imageSize < 1e9, "image is too large")
    val grayhistogram = new Array[Int](256)
    for (i <- 0 until grayhistogram.length) {
      grayhistogram(i) = 0
    }


    println("on est bien dans is Gray")
    var offset = 0
    val raster = image.getRaster
    for (h <- 0 until ih) {
      for (w <- 0 until iw) {
        val pix = raster.getSample(w, h, 0) & 0xFF
        grayhistogram(pix) += 1
      }
    }

    val histogram = new Array[Int](numberOfBins)
    for (bin <- 0 until numberOfBins) {
      histogram(bin) = grayhistogram(bin)
    }
    histogram
  }


  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("ImgHist").master("local[*]").getOrCreate()

    val numberOfBins=256

    val image_path = "car.jpg"

    val image_df = spark.read.format("image").load(image_path)
    val img_array = image_df.select(col("image")).rdd.flatMap(f => f.getAs[Array[Byte]]("data"))

    val hist = image_df.select(col("image.data"))
      .rdd.flatMap(f => f.getAs[Array[Byte]]("data")).map(r => r.toDouble).histogram(numberOfBins)

    //println("histogram RDD ")
    //for (i <- 0 until hist._1.length) {
    //  println(hist._1(i))
    //}
    // println("count RDD ")
    //for (i <- 0 until hist._2.length) {
    //  println(hist._2(i))
    //}



    val features_col = Array("image.data")
    val vector_assembler = new VectorAssembler()
      .setInputCols(features_col)
      .setOutputCol("features")

    val va_transformed_df = vector_assembler.transform(image_df)
    va_transformed_df.show(truncate=false)

    //val rdd_img_array = image_df.select(col("image.data"))
     // .rdd.flatMap(f => f.getAs[Array[Byte]]("data"))

   // val image_array = loadImageArray(path)

    //val image_df = spark.read.format("image").load(path)

    //image_df.show(truncate=false)

    //val img_array = image_df.select(col("image.data"))
    //  .rdd.flatMap(f => f.getAs[Array[Byte]]("data")).histogram(10)
    //println("image_array")
    //println(image_array)

    /*image_df.groupBy(col("w"), col("h"))
    .agg(collect_list(col("color")).as("color"))
    .orderBy(col("w"), col("h"))
    .rdd
    .map { f =>
      val a = f(2).asInstanceOf[WrappedArray[Int]]
      (f(0).toString.toInt, f(1).toString.toInt, a(0), a(1), a(2))
    }

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

  println("Height: %s, Width b: %s".format(height, width))

  var offSet = spark.sparkContext.longAccumulator("offSetAcc")
  var x = spark.sparkContext.longAccumulator("xAcc")
  var y = spark.sparkContext.longAccumulator("yAcc")
  x.add(1)
  y.add(1)*/


  }
}
*/