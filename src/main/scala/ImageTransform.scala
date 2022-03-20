
import ImageUtils._

import java.awt.Color
import java.awt.color.ColorSpace
import java.io.ByteArrayInputStream
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

object ImageTransform {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("imgSeg").master("local[*]").getOrCreate()

    val image_path = "car.jpg"

    val final_image_df = decodeImageDataFrame(spark, image_path)

    //val features_col = Array(
     // "_3","_4","_5")
    //val vector_assembler = new VectorAssembler()
    //  .setInputCols(features_col)
     // .setOutputCol("features")

    //val va_transformed_df = vector_assembler.transform(final_image_df)

    //final_image_df.show(truncate=false)

    // getting the histogram
    val counts = final_image_df



      .select(col("b"))
      .rdd.map(r => r.getDouble(0)).histogram(10)._1
    println(counts)
    //println(" Range: Array[Double]: %s, counts: Array[Long]: %s".format(ranges, counts))


  }

}
