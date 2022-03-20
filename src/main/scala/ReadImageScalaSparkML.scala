import java.awt.Color
import java.awt.color.ColorSpace
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO
import java.io.File
import scala.collection.JavaConverters._
import java.awt.image.BufferedImage

import org.apache.spark.annotation.Since
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object ReadImageScalaSparkML {

  val undefinedImageType = "Undefined"

  /**
   * (Scala-specific) OpenCV type mapping supported
   */
  val ocvTypes: Map[String, Int] = Map(
    undefinedImageType -> -1,
    "CV_8U" -> 0, "CV_8UC1" -> 0, "CV_8UC3" -> 16, "CV_8UC4" -> 24
  )

  /**
   * (Java-specific) OpenCV type mapping supported
   */
  val javaOcvTypes: java.util.Map[String, Int] = ocvTypes.asJava

  /**
   * Schema for the image column: Row(String, Int, Int, Int, Int, Array[Byte])
   */


  def loadImageArray(path: String): Array[Int] = {
    val org_img = ImageIO.read(new File(path))

    val img = new BufferedImage(org_img.getWidth, org_img.getHeight, BufferedImage.TYPE_BYTE_GRAY)
    println(img.getColorModel.getColorSpace.getType)
    println(ColorSpace.TYPE_GRAY)
    var array = Array[Int]()

    //if (img == null) {
    //  None
    //} else {
      val isGray = img.getColorModel.getColorSpace.getType == ColorSpace.TYPE_GRAY
      val hasAlpha = img.getColorModel.hasAlpha
      val height = img.getHeight
      val width = img.getWidth
      val (nChannels, mode) = if (isGray) {
        (1, ocvTypes("CV_8UC1"))
      } else if (hasAlpha) {
        (4, ocvTypes("CV_8UC4"))
      } else {
        (3, ocvTypes("CV_8UC3"))
      }
      println(nChannels+"___"+mode)

      val imageSize = height * width * nChannels
      println(height+"___"+width+"____"+nChannels)
      println(imageSize)

      assert(imageSize < 1e9, "image is too large")
      val decoded = Array.ofDim[Int](imageSize)

      // Grayscale images in Java require special handling to get the correct intensity
      if (isGray) {
        var offset = 0
        val raster = img.getRaster
        for (h <- 0 until height) {
          for (w <- 0 until width) {
            println("offset "+offset)
            decoded(offset) = raster.getSample(w, h, 0)
            offset += 1
            print("decoded(offset) "+decoded(offset))
          }
        }
      } else {
        var offset = 0
        for (h <- 0 until 5) {
          for (w <- 0 until 5) {
            val color = new Color(img.getRGB(w, h), hasAlpha)
            println("offset "+offset)
            decoded(offset) = color.getBlue
            println("color.getBlue "+color.getBlue)
            decoded(offset + 1) = color.getGreen
            println("color.getGreen "+color.getGreen)
            decoded(offset + 2) = color.getRed
            println("color.getRed "+color.getRed)
            if (hasAlpha) {
              decoded(offset + 3) = color.getAlpha.toByte
            }
            offset += nChannels
          }
        }
      }

   /* val image = ImageIO.read(new File(path))

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
      }*/

      decoded

    }

    def main(args: Array[String]) {
      //val spark = SparkSession.builder().appName("imgSeg").master("local[*]").getOrCreate()

      val path = "snow.jpg"

      val image_array = loadImageArray(path)

      //val image_df = spark.read.format("image").load(path)

      //image_df.show(truncate=false)

      //val img_array = image_df.select(col("image.data"))
      //  .rdd.flatMap(f => f.getAs[Array[Byte]]("data")).histogram(10)
      println("image_array")
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
