import java.awt.Color
import java.awt.color.ColorSpace
import java.io.{ByteArrayInputStream, File}
import javax.imageio.ImageIO
import scala.collection.JavaConverters._
import org.apache.spark.annotation.Since
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, collect_list}

import javax.imageio.ImageIO
import scala.collection.mutable.WrappedArray

import org.apache.spark.sql.functions.{col,when,lit}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util._
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasInputCol,HasOutputCol,HasHandleInvalid}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{StructType,StructField,DoubleType}
import org.apache.spark.SparkException



object ReadImageScalaSpark {

  //var filepath = "snow.jpg"
  def printPixelARGB(pixel: Int): (Int, Int, Int, Int) = {
    val alpha = (pixel >> 24) & 0xff;
    val red = (pixel >> 16) & 0xff;
    val green = (pixel >> 8) & 0xff;
    val blue = (pixel) & 0xff;
    (alpha, red, green, blue)
  }



  /* import org.apache.spark.InternalAccumulator.input
  import java.awt.Color
  import java.util

  val rhistogram = new Array[Int](256)
  val ghistogram = new Array[Int](256)
  val bhistogram = new Array[Int](256)

  for (i <- 0 until rhistogram.length) {
    rhistogram(i) = 0
  }
  for (i <- 0 until ghistogram.length) {
    ghistogram(i) = 0
  }
  for (i <- 0 until bhistogram.length) {
    bhistogram(i) = 0
  }

  var i = 0
  while ( {
    i < input.getWidth
  }) {
    var j = 0
    while ( {
      j < input.getHeight
    }) {
      val red = new Color(input.getRGB(i, j)).getRed
      val green = new Color(input.getRGB(i, j)).getGreen
      val blue = new Color(input.getRGB(i, j)).getBlue
      // Increase the values of colors
      rhistogram(red) += 1
      ghistogram(green) += 1
      bhistogram(blue) += 1

      j += 1
    }

    i += 1
  }

  val hist = new util.ArrayList[Array[Int]]
  hist.add(rhistogram)
  hist.add(ghistogram)
  hist.add(bhistogram)


import java.awt.image.BufferedImage
import java.awt.image.WritableRaster
def getGrayscaleImage(src: BufferedImage): BufferedImage =  {
val gImg: BufferedImage = new BufferedImage(src.getWidth, src.getHeight, BufferedImage.TYPE_BYTE_GRAY)
val wr: WritableRaster = src.getRaster
val gr: WritableRaster = gImg.getRaster
for (i <- 0 until wr.getWidth)  {
  for (j <- 0 until wr.getHeight)  {
    gr.setSample(i, j, 0, wr.getSample(i, j, 0))
  }
}
gImg.setData(gr)
return gImg
}


*/
  def loadImageArray(path: String): Array[PixelInfo] = {
    ImageIO.setUseCache(false)

    val image = ImageIO.read(new File(path))

    // obtain width and height of image
    val w = image.getWidth
    val h = image.getHeight

    var array = Array[PixelInfo]()
    var cont = 1
    for (x <- 0 until w)
      for (y <- 0 until h) {
        val argba = printPixelARGB(image.getRGB(x, y))
        println(x+","+y)
        println(argba)
        array = array :+ PixelInfo(image.getRGB(x, y), argba._1, argba._2, argba._3, argba._4, x, y)
        cont += 1
      }
    array

  }


  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("imgSeg").master("local[*]").getOrCreate()

    val path="snow.jpg"

    //val image_array = loadImageArray(path)

    val image_df = spark.read.format("image").load(path)

    //image_df.show(truncate=false)

    val img_array = image_df.select(col("image.data"))
      .rdd.flatMap(f => f.getAs[Array[Byte]]("data")).histogram(10)
    println("image_array")
    println(img_array)

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
