
import org.apache.spark.streaming._

import org.apache.spark.streaming.StreamingContext

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, BasicAWSCredentials}
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.regions.RegionUtils
import org.apache.spark.streaming.kinesis.KinesisUtils
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode.Append
import org.elasticsearch.spark.sql._
import org.apache.spark.sql.SQLContext


/**
 * Author : Shashi Vishwakarma
 * Description : Streaming Solution for deciding Surge price based on Supply and Demand for Geo Hash Location.
 * Date : 1st December 2018
 * 
 */


class SurgeStreamProcessor {

	def main(args: Array [String]) {
		// Check that all required args were passed in.
		if (args.length != 3) {
			System.err.println("")

			System.exit(1)
		}


		    val Array(appName, streamName, endpointUrl) = args

				val endpointUrl = "https://kinesis.us-east-1.amazonaws.com"
				val steamName = "test"

				val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
				require(credentials != null,
				"No AWS credentials found. Please specify credentials using one of the methods specified " +
						"in http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html")
				val kinesisClient = new AmazonKinesisClient(credentials)
				kinesisClient.setEndpoint("https://kinesis.eu-central-1.amazonaws.com")
				val numShards = kinesisClient.describeStream("spark-demo").getStreamDescription().getShards().size

				val numStreams = numShards
				val batchInterval = Seconds(10)
				val kinesisCheckpointInterval = batchInterval
				val regionName = RegionUtils.getRegionByEndpoint(endpointUrl).getName()


				val sparkConfig = new SparkConf().setAppName("GrabKinesisConsumer")
				
				/**
				 * Elastic Search Configuration
				 */
				sparkConfig.set("es.index.auto.create", "true")
				sparkConfig.set("spark.es.nodes", "localhost");
		    sparkConfig.set("es.port", "9200");
		    
		  

				val ssc = new StreamingContext(sparkConfig, batchInterval)

				// Create the Kinesis DStreams
				val kinesisStreams = (0 until numStreams).map { i =>
				KinesisUtils.createStream(ssc, "grabDPDataProcessor", steamName, endpointUrl, regionName,InitialPositionInStream.LATEST, kinesisCheckpointInterval, StorageLevel.MEMORY_AND_DISK_2)
		}

				// Union all the streams
		    val unionStreams = ssc.union(kinesisStreams)

				//Schema of the incoming data on Kinesis Stream
				val schemaString = "typeOfUser,pickup_datetime,pickup_langitude,pickup_lattitude"


				//Parse the data
				val tableSchema = StructType( schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))


				//Processing each RDD and storing it in temporary table
				unionStreams.foreachRDD ((rdd: RDD[Array[Byte]], time: Time) => {
				  
					    val rowRDD = rdd.map(w => Row.fromSeq(new String(w).split(",")))				
					
					    val wordsDF = sqlContext.createDataFrame(rowRDD,tableSchema)
							
					    wordsDF.registerTempTable("realTimeDriverPassangerData")

							val surgePriceResult = "select geoHash , case when passangerCoutInGeoHashfrom > driverCoutInGeoHash then passangerCoutInGeoHashfrom/driverCoutInGeoHash else '0' END AS surgePrice from " 
									+ " ((select geoHash , count(*) AS driverCoutInGeoHash from driverPassangerStream group by geoHash where typeOfUser ='driver') D "
									+ "  inner join  "
									+ "(select geoHash , count(*)  AS passangerCoutInGeoHashfrom driverPassangerStream group by geoHash where typeOfUser ='passanger') P ) on D.geoHash=P.geoHash )"
							
							surgePriceResultDF = sqlContext.sql(surgePriceResult)
									
							val surgePriceResultRDD: RDD[Row] = surgePriceResultDF.rdd

							surgePriceResultRDD.saveToEs("gyms/gym")


				}

			
				ssc.start()
				ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 5 * 1000)


	}
}