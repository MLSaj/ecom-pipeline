import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType,IntegerType,DoubleType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j._
import scala.util.Random
import org.apache.spark.sql.functions.{col, udf}
import scala.math.BigDecimal

object kafkaSparkConnection {

  val spark = SparkSession.builder()
    .appName("Stream processing application")
    .master("local[*]")
    .getOrCreate()

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


//    spark.udf.register(
//      "random_card_type",
//      (transaction_card_type_list:Array[String]) =>
//        random_card(transaction_card_type_list)
//    )
    //spark.udf.register("random_card_choice", random_card)

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
      //concat(col("username"), round(rand() * 1000, 0).cast(IntegerType())).alias("user_id"),
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
      random_card_choice(col("tran_card_type")).alias("tran_card_type")
      //((col("tran_amount")* get_random)% 0.01).alias("tran_amount")
      //getRandomCard(col("tran_card_type")).alias("tran_card_type")
      //concat(col("product_id"), round(rand() * 100, 0).cast(IntegerType())).alias("product_id"),
      //round(rand() * col("tran_amount"), 2).alias("tran_amount")
    )

    trans_df_6
          .writeStream
          .format("console")
          .outputMode("append")
          .start()
          .awaitTermination()










  }


}