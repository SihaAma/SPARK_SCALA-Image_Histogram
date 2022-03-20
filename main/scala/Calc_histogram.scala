
import ImageUtils._
import ReadImageScalaSparkML.{ocvTypes, undefinedImageType}

import java.awt.Color
import java.awt.color.ColorSpace
import java.io.{ByteArrayInputStream, File}
import javax.imageio.ImageIO
import scala.collection.JavaConverters._
import org.apache.spark.annotation.Since
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum, udf}
import java.awt.image.BufferedImage
import java.awt.image.DataBufferByte
import java.awt.image.DataBufferUShort
import java.io.File
import java.awt.image.BufferedImage
import java.util.ArrayList


/**
 * What does this program do?
 * Loads an image
 * Convert the original image to grayscale
 * Calculates and returns the histogram for the image. The histogram is
 * represented by an int array of 256 elements. Each element gives the number
 * of pixels in the image of the value equal to the index of the element.
 *@author Siham Amara
 *@version Jan 2022
 **/



object Calc_histogram {

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
  def getHistogram(imagebyte:Array[Byte]): Array[Int]= {

    val bytetoimage = new ByteArrayInputStream(imagebyte)
    val img = ImageIO.read(bytetoimage)
    val image=makeGray(img)

    val iw = image.getWidth
    val ih = image.getHeight
    val pixels = Array.ofDim[Int](iw,ih)
    val imageSize = ih * iw
    assert(imageSize < 1e9, "image is too large")
    val grayhistogram = new Array[Int](256)
    val rhistogram = new Array[Int](256)
    val ghistogram = new Array[Int](256)
    val bhistogram = new Array[Int](256)
    val histogram = new ArrayList[Array[Int]]



    for (i <- 0 until rhistogram.length) {
      rhistogram(i) = 0
    }
    for (i <- 0 until ghistogram.length) {
      ghistogram(i) = 0
    }
    for (i <- 0 until bhistogram.length) {
      bhistogram(i) = 0
    }


    //val isGray=image.getType == 10
    //val isGray = image.getColorModel.getColorSpace.getType == ColorSpace.TYPE_GRAY
    val isGray=true
    println(image.getColorModel.getColorSpace.getType)

    if (isGray) {
      println("on est bien dans is Gray")
      var offset = 0
      val raster = image.getRaster
      for (h <- 0 until ih) {
        for (w <- 0 until iw) {
          //pixels(w)(h) = raster.getSample(w, h, 0)& 0xFF
          val pix = raster.getSample(w, h, 0)& 0xFF
          //println(raster.getSample(w, h, 0)& 0xFF)
          grayhistogram(pix) += 1
        }
      }
      histogram.add(grayhistogram)
    }
    else {
      for (h <- 0 until ih) {
        for (w <- 0 until iw) {
          val color = new Color(image.getRGB(w, h))
          val blue = color.getBlue
          //println("blue "+blue)
          val green = color.getGreen
          val red = color.getRed
          // Increase the values of colors
          rhistogram(red) += 1
          ghistogram(green) += 1
          bhistogram(blue) += 1
        }
      }
      histogram.add(rhistogram)
      histogram.add(ghistogram)
      histogram.add(bhistogram)
    }
    val numberOfBins=256

    val hist =histogram.get(0)
    val dest = new Array[Int](numberOfBins)
    for (bin <- 0 until numberOfBins) {
      dest(bin) = hist(bin)
    }
    dest

  }



  def main(args: Array[String]): Unit = {
    val path = "car.jpg"
    val grey_scales = 256
    val org_img = ImageIO.read(new File(path))
    //val img = new BufferedImage(org_img.getWidth, org_img.getHeight, BufferedImage.TYPE_BYTE_GRAY)
    val img = makeGray(org_img)

    //val histogram = getHistogram(img,0)

    //histogram.foreach { println }


  }
}
