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
					import spark.implicits._

					val incremental_df = spark
					.read
					.format("jdbc")
					.option("url","jdbc:mysql://localhost:3306/customer_web_analytics?characterEncoding=utf8")
					.option("driver","com.mysql.jdbc.Driver")
					.option("user","root")
					.option("password","root")
					.option("dbtable","incremental_log")
					.load()

					val incremental_list = incremental_df.collectAsList().get(0)
					var last_value = incremental_list.get(0)

					/*var last_value = incremental_df
					.orderBy(col("lastvalue").desc)
					//.sort(col("lastvalue").desc)
					//.sort(desc("lastvalue"))
					.map(x=>x.getInt(0))
					.first()*/


					//incremental_df.printSchema()
					//incremental_df.show()

					//val table_name = incremental_list.get(1).toString()
					val table_name = "web_increment_data_src_log_dlm"


					val filedata = spark
					.read
					.format("jdbc")
					.option("url","jdbc:mysql://localhost:3306/customer_web_analytics?characterEncoding=utf8")
					.option("driver","com.mysql.jdbc.Driver")
					.option("user","root")
					.option("password","root")
					.option("dbtable",table_name)
					.load()
					.filter(col("id") > last_value)

					if (filedata.count() > 0){
            println("--------------file data frame----------------")
						filedata.show()

						val randomdata = Source.fromURL("https://randomuser.me/api/0.8/?results=1000").mkString
						val jsonrdd = sc.parallelize(List(randomdata))
						val jsondf = spark.read.json(jsonrdd)
						println("--------------json api data frame----------------")
						jsondf.show()

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
						.mode("Append")
						.save("file:///C:/Users/Rupesh/Desktop/Rupesh/zeyoborn/Spark_output_save/20DEC2020/notnull_data")

						null_result.coalesce(1).write.format("json")
						.partitionBy("create_date")
						.mode("Append")
						.save("file:///C:/Users/Rupesh/Desktop/Rupesh/zeyoborn/Spark_output_save/20DEC2020/null_data")
						println("----------writing completed------------------")
						

						//Update last value in table log
						//filedata.printSchema()
						//this code not working need to find reason
						//last_value = filedata.selectExpr("max(id) as max_id").collectAsList().get(0).getInt(0)
						//val update_incremental_df = incremental_df.withColumn("lastvalue", lit(last_value))

						val update_incremental_df = filedata.selectExpr("max(id) as lastvalue")
						last_value = update_incremental_df.map(x=>x.getInt(0)).first()
						update_incremental_df.printSchema()
						update_incremental_df.show()

						update_incremental_df
						.write
						.format("jdbc")
						.option("url","jdbc:mysql://localhost:3306/customer_web_analytics?characterEncoding=utf8")
						.option("driver","com.mysql.jdbc.Driver")
						.option("user","root")
						.option("password","root")
						.option("dbtable","incremental_log")
						//.mode("Append")
						.mode("overwrite")
						.save()
						println("writing completed and update last value = "+last_value.toString())
						
					}
					else{
						println("no data for update")
						spark.close()
					}

	}
}