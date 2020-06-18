import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j._

import scala.util.Random
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.cassandra._





object kafkaSparkConnection {

  val spark = SparkSession.builder()
    .appName("Stream processing application")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val trans_message_schema: StructType = StructType(Array(
    StructField("results", ArrayType(StructType(Array(
      StructField("user", StructType(Array(
        StructField("gender", StringType),
        StructField("name", StructType(Array(
          StructField("title", StringType),
          StructField("first", StringType),
          StructField("last", StringType)
        ))),
        StructField("location", StructType(Array(
          StructField("street", StringType),
          StructField("city", StringType),
          StructField("state", StringType),
          StructField("zip", IntegerType)
        ))),
        StructField("email", StringType),
        StructField("username", StringType),
        StructField("password", StringType),
        StructField("salt", StringType),
        StructField("md5", StringType),
        StructField("sha1", StringType),
        StructField("sha256", StringType),
        StructField("registered", IntegerType),
        StructField("dob", IntegerType),
        StructField("phone", StringType),
        StructField("cell", StringType),
        StructField("PPS", StringType),
        StructField("picture", StructType(Array(
          StructField("large", StringType),
          StructField("medium", StringType),
          StructField("thumbnail", StringType))))

      )))
    )), true)),
    StructField("nationality", StringType),
    StructField("seed", StringType),
    StructField("version", StringType),
    StructField("tran_detail", StructType(Array(
      StructField("product_id", StringType),
      StructField("tran_card_type",  ArrayType(StringType, true), true),
      StructField("tran_amount", DoubleType)
    )))
  ))

  // MongoDB Cluster Details
  val mongodb_host_name = "35.199.22.78"
  val mongodb_port_no = "27017"
  val mongodb_user_name = "demouser"
  val mongodb_password = "demouser"
  val mongodb_database_name = "trans_db"

  val cassandra_connection_host = "35.199.22.78"
  //val cassandra_connection_host = "10.150.0.9"
  val cassandra_connection_port = "9042"
  val cassandra_keyspace_name = "trans_ks"
  val cassandra_table_name = "trans_message_detail_tbl"

  def save_to_cassandra_table = (current_df:DataFrame,epoc_id:Long) => {
    print("Inside save_to_cassandra_table function")
    print("Printing epoc_id: ")
    print(epoc_id)

    current_df
    .write
    .format("org.apache.spark.sql.cassandra")
    .mode("append")
    .option("spark.cassandra.connection.host", cassandra_connection_host)
    .option("spark.cassandra.connection.port", cassandra_connection_port)
    .option("keyspace", cassandra_keyspace_name)
    .option("table", cassandra_table_name)
    .save()
    print("Exit out of save_to_cassandra_table function")

  }


  val KAFKA_TOPIC_NAME_CONS = "transmessage"
  //val random = new Random(100);

  def main(args: Array[String]): Unit = {
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "35.199.22.78:9092")
      .option("subscribe", KAFKA_TOPIC_NAME_CONS)
      .option("startingOffsets", "latest")
      .load()

    println("Printing Schema of trans_message_df: ")
    kafkaDF.printSchema()

    val trans_df_1 = kafkaDF.selectExpr("CAST(value AS STRING)")

//        trans_df_1
//          .writeStream
//          .format("console")
//          .outputMode("append")
//          .start()
//          .awaitTermination()



    val trans_df_2 = trans_df_1.select(from_json(col("value"), trans_message_schema)
      .as("message_detail"))




//    trans_df_2
//      .writeStream
//      .format("console")
//      .outputMode("append")
//      .start()
//      .awaitTermination()

    val trans_df_3 = trans_df_2.select("message_detail.*")




    trans_df_3.printSchema()

//    trans_df_3
//      .writeStream
//      .format("console")
//      .outputMode("append")
//      .start()
//      .awaitTermination()


    val trans_df_4 = trans_df_3.select(explode(col("results.user")).alias("user"),
      col("nationality"),
      col("seed"),
      col("version"),
      col("tran_detail.tran_card_type").alias("tran_card_type"),
      col("tran_detail.product_id").alias("product_id"),
      col("tran_detail.tran_amount").alias("tran_amount")
    )


    val trans_df_5 = trans_df_4.select(
      col("user.gender"),
      col("user.name.title"),
      col("user.name.first"),
      col("user.name.last"),
      col("user.location.street"),
      col("user.location.city"),
      col("user.location.state"),
      col("user.location.zip"),
      col("user.email"),
      col("user.username"),
      col("user.password"),
      col("user.salt"),
      col("user.md5"),
      col("user.sha1"),
      col("user.sha256"),
      col("user.registered"),
      col("user.dob"),
      col("user.phone"),
      col("user.cell"),
      col("user.PPS"),
      col("user.picture.large"),
      col("user.picture.medium"),
      col("user.picture.thumbnail"),
      col("nationality"),
      col("seed"),
      col("version"),
      col("tran_card_type"),
      col("product_id"),
      col("tran_amount")
    )


//    trans_df_5
//      .writeStream
//      .format("console")
//      .outputMode("append")
//      .start()
//      .awaitTermination()


    def random_card = (transaction_card_type_list:Seq[String]) => {
      val random = new Random
      //transaction_card_type_list.toArray
      if(transaction_card_type_list != null){
        transaction_card_type_list(random.nextInt(transaction_card_type_list.length))
      }
      else
        "MasterCard"
    }

    val random_card_choice = udf(
      (transaction_card_type_list:Seq[String]) => random_card(transaction_card_type_list)
      ,StringType)








    val trans_df_6 = trans_df_5.select(
      col("gender"),
      col("title"),
      col("first").alias("first_name"),
      col("last").alias("last_name"),
      col("street"),
      col("city"),
      col("state"),
      col("zip"),
      col("email"),
      concat(col("username"), round(rand() * 1000, 0).cast(IntegerType)).alias("user_id"),
      col("password"),
      col("salt"),
      col("md5"),
      col("sha1"),
      col("sha256"),
      col("registered"),
      col("dob"),
      col("phone"),
      col("cell"),
      col("PPS"),
      col("large"),
      col("medium"),
      col("thumbnail"),
      col("nationality"),
      col("seed"),
      col("version"),
      random_card_choice(col("tran_card_type")).alias("tran_card_type"),
      concat(col("product_id"), round(rand() * 100, 0).cast(IntegerType)).alias("product_id"),
      round(rand() * col("tran_amount"), 2).alias("tran_amount")
    )


    val trans_df_7 = trans_df_6.withColumn("tran_date",
      from_unixtime(col("registered"), "yyyy-MM-dd HH:mm:ss"))
//
//      trans_df_6
//        .writeStream
//        .format("console")
//        .outputMode("append")
//        .start()
//        .awaitTermination()

    val trans_df_8 = trans_df_7.select(
      col("user_id"),
      col("first_name"),
      col("last_name"),
      col("gender"),
      col("city"),
      col("state"),
      col("zip"),
      col("email"),
      col("nationality"),
      col("tran_card_type"),
      col("tran_date"),
      col("product_id"),
      col("tran_amount"))

    trans_df_8
    .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("update")
      .foreachBatch(save_to_cassandra_table)
      .start()
    // Data Processing/Data Transformation
    val trans_df_9 = trans_df_8.withColumn("tran_year",
      year(to_timestamp(col("tran_date"), "yyyy")))

    val year_wise_total_sales_count_df = trans_df_9
      .groupBy("tran_year")
      .agg(count("tran_year").alias("tran_year_count"))

    val country_wise_total_sales_count_df = trans_df_9
      .groupBy("nationality")
      .agg(count("nationality").alias("tran_country_count"))

    val card_type_wise_total_sales_count_df = trans_df_9
      .groupBy("tran_card_type")
      .agg(count("tran_card_type").alias("tran_card_type_count"))

    val card_type_wise_total_sales_df = trans_df_9
      .groupBy("tran_card_type")
      .agg(sum("tran_amount").alias("tran_card_type_total_sales"))

    val year_country_wise_total_sales_df = trans_df_9
      .groupBy("tran_year","nationality")
      .agg(sum("tran_amount").alias("tran_year_country_total_sales"))

    // Writing Aggregated DataFrame into MongoDB Collection Starts Here
    val mongodb_collection_name = "year_wise_total_sales_count"
    val spark_mongodb_output_uri = "mongodb://" + mongodb_user_name + ":" + mongodb_password + "@" + mongodb_host_name + ":" + mongodb_port_no + "/" + mongodb_database_name + "." + mongodb_collection_name
    println("Printing spark_mongodb_output_uri: " + spark_mongodb_output_uri)

    year_wise_total_sales_count_df.writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        //val batchDF_1 = batchDF.withColumn("batch_id", lit(batchId))
        // Transform batchDF and write it to sink/target/persistent storage
        // Write data from spark dataframe to database

        batchDF.write
          .format("mongo")
          .mode("append")
          .option("uri", spark_mongodb_output_uri)
          .option("database", mongodb_database_name)
          .option("collection", mongodb_collection_name)
          .save()
      }.start()

    val mongodb_collection_name_1 = "country_wise_total_sales_count"
    val spark_mongodb_output_uri_1 = "mongodb://" + mongodb_user_name + ":" + mongodb_password + "@" + mongodb_host_name + ":" + mongodb_port_no + "/" + mongodb_database_name + "." + mongodb_collection_name
    println("Printing spark_mongodb_output_uri: " + spark_mongodb_output_uri)

    country_wise_total_sales_count_df.writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        //val batchDF_1 = batchDF.withColumn("batch_id", lit(batchId))
        // Transform batchDF and write it to sink/target/persistent storage
        // Write data from spark dataframe to database

        batchDF.write
          .format("mongo")
          .mode("append")
          .option("uri", spark_mongodb_output_uri_1)
          .option("database", mongodb_database_name)
          .option("collection", mongodb_collection_name_1)
          .save()
      }.start()
    // Writing Aggregated DataFrame into MongoDB Collection Ends Here

    // Write final result into console for debugging purpose
    val trans_detail_write_stream = year_wise_total_sales_count_df
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("update")
      .option("truncate", "false")
      .format("console")
      .start()

    trans_detail_write_stream.awaitTermination()

    println("Real-Time Data Pipeline Completed.")



//    trans_df_8
//    .writeStream
//    .trigger(Trigger.ProcessingTime("5 seconds"))
//      .outputMode("update")
//    .option("truncate", "false")
//    .format("console")
//    .start()










  }


}
