import org.apache.spark.sql.functions.{aggregate, col}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{BinaryType, LongType, StringType, StructField, StructType}

object Parquet {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[16]").getOrCreate()

    //write(spark)

    val df = read(spark)
    df.printSchema()

    println(spark.sparkContext.wholeTextFiles("data.parquet")
      .foreach(x =>
      println(x._1)
    ))
      //.wholeTextFiles("data.parquet")
    //df.withColumn("id", col("id").cast(StringType)).show()
  }

  private def read(spark: SparkSession) = {
    val schema = StructType(
      Array(
        StructField("id", BinaryType, true),
        StructField("data", StringType, true),
      )
    )

    //spark.read.schema(schema).parquet("data.parquet")
    spark.read
      //.schema(schema)
      //.option("binaryAsString", value=true)
      //.option("mergeSchema", value=true)
      .parquet("data.parquet")
  }

  private def write(spark: SparkSession) = {
    val simpleData = Seq(
      Row("10", "data1"),
      Row("20", "data2"),
    )

    val schema = StructType(
      Array(
        StructField("id", StringType, true),
        StructField("data", StringType, true),
      )
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(simpleData), schema)

    df.write
      .mode("append")
      .parquet("data.parquet")
  }
}