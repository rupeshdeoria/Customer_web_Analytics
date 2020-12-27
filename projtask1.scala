package Project_pack1
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.functions.length
object projtask1 {
	def main(args:Array[String]):Unit={
			val spark = SparkSession
					.builder()
					.appName("SPARKPROJECT")
					.master("local[*]")
					.enableHiveSupport()
					.getOrCreate()

					val sc = spark.sparkContext
					sc.setLogLevel("Error")


					val schema_struct = StructType(StructField("id",StringType)
							::StructField("username",StringType)
							::StructField("amount",StringType)
							::StructField("ip",StringType)
							::StructField("createdt",StringType)
							::StructField("value",StringType)
							::StructField("score",StringType)
							::StructField("regioncode",StringType)
							::StructField("status",StringType)
							::StructField("method",StringType)
							::StructField("key",StringType)
							::StructField("count",StringType)
							::StructField("type",StringType)
							::StructField("site",StringType)
							::StructField("statuscode",StringType)
							::Nil)

					val filedata = spark.read.schema(schema_struct).format("csv").load("file:///C:/Users/Rupesh/Desktop/Rupesh/zeyoborn/Data_Set/1")
					println("--------------file data frame----------------")
					filedata.show()
					
					val randomdata = Source.fromURL("https://randomuser.me/api/0.8/?results=1000").mkString
					val jsonrdd = sc.parallelize(List(randomdata))
					val jsondf = spark.read.json(jsonrdd)
					println("--------------json api data frame----------------")
					jsondf.show()
					
					
					/*val jsondf = spark.read.format("json")
					.option("multiLine", "true")
					.load("file:///C:/Users/Rupesh/Desktop/Rupesh/zeyoborn/Data_Set/randomuser.json")*/
					//jsondf.printSchema()
					
					//jsondf.show(10)
					
					
					println("--------------flatten json api data frame----------------")
					val flatten_df = jsondf
					.withColumn("results", explode(col("results")))
					.select("nationality", "seed" , "version", "results.user.username", "results.user.cell", "results.user.dob", "results.user.salt")
					.withColumn("username", regexp_replace(col("username"), "[0-9]", ""))
					flatten_df.show()
					
					val w = Window.partitionBy(col("username")).orderBy(col("username"))
					val rankdf = flatten_df
					.withColumn("rank", row_number().over(w))
					.filter(col("rank") === "1")
					//.drop("rank")
					
					println("--------------select first occurance data----------------")
					rankdf.printSchema()
					rankdf.show()
					
					val comdf = filedata.join(broadcast(rankdf),Seq("username"),"left")
					.filter(col("salt").isNotNull)
					
					
					println("------------------not null data frame-----------------------")
					comdf.orderBy(col("username")).show()
									
					
					val comdf2 = filedata.join(broadcast(rankdf),Seq("username"),"left")
					.filter(col("salt").isNull)
					.na.fill("Not Availabel")
					.na.fill(0)
					
					println("------------------null data frame-----------------------")
					comdf2.orderBy(col("username")).show()
					
					
										
					val notnull_result = comdf2.groupBy(col("username"))
					.agg(collect_list(col("id")).alias("id")
					    ,collect_list(col("ip")).alias("ip")
					    ,sum("amount").cast(DecimalType(20,2)).alias("total_amount")
					    ,struct(count(col("id")).alias("id_count"), count(col("ip")).alias("ip_count"))
					    .alias("counts")
					).withColumn("create_date", current_date)
					
					println("------------------final result not null data frame-----------------------")
					notnull_result.printSchema()
					notnull_result.show()
					
					
					val null_result = comdf.groupBy(col("username"))
					.agg(collect_list(col("id")).alias("id")
					    ,collect_list(col("ip")).alias("ip")
					    ,sum("amount").cast(DecimalType(20,2)).alias("total_amount")
					    ,struct(count(col("id")).alias("id_count"), count(col("ip")).alias("ip_count"))
					    .alias("counts")
					).withColumn("create_date", current_date)
					
					
					
					println("------------------final result for null data frame-----------------------")
					null_result.printSchema()
					null_result.show()
					
					
					println("--------------writing result-----------------")
					notnull_result.coalesce(1).write.format("json")
					.partitionBy("create_date")
					.mode("Overwrite")
					.save("file:///C:/Users/Rupesh/Desktop/Rupesh/zeyoborn/Spark_output_save/20DEC2020/notnull_data")
					
					null_result.coalesce(1).write.format("json")
					.partitionBy("create_date")
					.mode("Overwrite")
					.save("file:///C:/Users/Rupesh/Desktop/Rupesh/zeyoborn/Spark_output_save/20DEC2020/null_data")
					println("----------writing completed------------------")
	}
}