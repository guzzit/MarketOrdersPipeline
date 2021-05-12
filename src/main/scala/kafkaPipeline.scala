import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, count, from_json, sum}
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}

object kafkaPipeline {
  @throws[StreamingQueryException]
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    System.setProperty("java.io.tmpdir", "C://temp")

    val spark = SparkSession.builder.appName("Simple Application")
      .config("spark.master", "local")
      .config("spark.hadoop.fs.defaultFS","hdfs://localhost:9870")
      .getOrCreate

    val kafkaSub = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe", "marketorders-events")
      .option("failOnDataLoss", "false")
      .load()

    val query = kafkaSub.writeStream
      .outputMode("append")
      .option("checkpointLocation", "hdfs://localhost:9000/RddCheckPoint")
      .format("console").start

    val jsonString = kafkaSub.selectExpr("CAST(value AS STRING)")

    val schema = new StructType()
      .add("timestamp", TimestampType)
      .add("symbol",StringType)
      .add("bid_price",StringType)
      .add("order_quantity",IntegerType)
      .add("trade_type",StringType)

    val jsonDataSet = jsonString.select(from_json(col("value"), schema)
      .as("data"))
      .select("data.*")
      .withWatermark("timestamp", "20000 milliseconds")

    var query2 = jsonDataSet.groupBy("symbol")
      .agg(sum("order_quantity"), count("symbol"))

    query2.writeStream
      .format("console")
      .outputMode("update")
      .option("checkpointLocation", "hdfs://localhost:9000/RddCheckPoint4")
      .start()
      .awaitTermination()

    query2
      .selectExpr("marketOrdersAnalysis-events", "CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "hdfs://localhost:9000/RddCheckPoint4")
      //.option("topic", "marketOrdersAnalysis-events")
      .start
      .awaitTermination()

    query.awaitTermination()


  }
}
