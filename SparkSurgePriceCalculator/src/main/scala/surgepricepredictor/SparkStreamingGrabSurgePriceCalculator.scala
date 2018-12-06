package surgepricepredictor

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, BasicAWSCredentials}
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.regions.RegionUtils
import org.apache.spark.streaming.kinesis.KinesisUtils
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.SQLContext

import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark





/**
 * Author : Shashi Vishwakarma
 * Description : Streaming Solution for deciding Surge price based on Supply and Demand for Geo Hash Location.
 * Date : 1st December 2018
 * 
 */

object SparkStreamingGrabSurgePriceCalculator {
	def main(args: Array[String]) {

		/*if (args.length < 2) {
			System.err.println("Usage : ")
			System.exit(1)
		}*/


		/**
		 * Kinesis Connection Configuration
		 */

		    val endpointUrl = "https://kinesis.eu-central-1.amazonaws.com"
				val streamName = "test"

				val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
				require(credentials != null,
				"No AWS credentials found. Please specify credentials using one of the methods specified " +
						"in http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html")
				val kinesisClient = new AmazonKinesisClient(credentials)
				kinesisClient.setEndpoint("https://kinesis.eu-central-1.amazonaws.com")
				val numShards = kinesisClient.describeStream("test").getStreamDescription().getShards().size

				val numStreams = numShards
				val batchInterval = Seconds(10)
				val kinesisCheckpointInterval = batchInterval
				//val regionName = RegionUtils.getRegionByEndpoint(endpointUrl).getName()
				val regionName = "eu-central-1"

				/**
				 * Spark Streaming Configuration
				 */

				val sparkConfig = new SparkConf().setAppName("SparkStreamingGrabSurgePriceCalculator")

				/**
				 * Elastic Search Configuration
				 */
				sparkConfig.set("es.index.auto.create", "true")
				sparkConfig.set("spark.es.nodes", "localhost")
				sparkConfig.set("es.port", "9200")

				val ssc = new StreamingContext(sparkConfig, batchInterval)

				val kinesisStreams = (0 until numStreams).map { i =>
				KinesisUtils.createStream(ssc, "grabDPDataProcessor", streamName, endpointUrl, regionName,InitialPositionInStream.LATEST, kinesisCheckpointInterval, StorageLevel.MEMORY_AND_DISK_2)
		}

		    // Union all the streams
	    	val unionStreams = ssc.union(kinesisStreams)

				//Schema of the incoming data on Kinesis Stream
				val schemaString = "typeOfUser,pickup_datetime,pickup_langitude,pickup_lattitude"


				//Parse the data
				val tableSchema = StructType( schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))


				/**
				 * Processing DStream 
				 */


				unionStreams.foreachRDD ((rdd: RDD[Array[Byte]]) => {
					    val sc = rdd.context
							val sqlContext = new SQLContext(sc)
				  		val rowRDD = rdd.map(w => Row.fromSeq(new String(w).split(",")))	
				  		
				  		println("Printing Data  " + rowRDD.count())
				  		
							val wordsDF = sqlContext.createDataFrame(rowRDD,tableSchema)
							wordsDF.registerTempTable("driverpassangerstream")

							
							val surgePriceResult =  "select * from driverpassangerstream limit 10"
							//val surgePriceResult = "select geoHash , case when passangerCoutInGeoHashfrom > driverCoutInGeoHash then passangerCoutInGeoHashfrom/driverCoutInGeoHash else '0' END AS surgePrice from ((select geoHash , count(*) AS driverCoutInGeoHash from driverPassangerStream group by geoHash where typeOfUser ='driver') D inner join (select geoHash , count(*)  AS passangerCoutInGeoHashfrom driverPassangerStream group by geoHash where typeOfUser ='passanger') P ) on D.geoHash=P.geoHash ) "
						
							val surgePriceResultDF = sqlContext.sql(surgePriceResult)
							
							println("=================================================================")
							println("Printing Query Result " + surgePriceResultDF.show())
							println("=================================================================")
							
							val surgePriceResultRDD: RDD[Row] = surgePriceResultDF.rdd
						
						//	EsSpark.saveToEs(surgePriceResultRDD, "gyms/gym")
						
				})

				ssc.start()
				ssc.awaitTermination()
	}
}
