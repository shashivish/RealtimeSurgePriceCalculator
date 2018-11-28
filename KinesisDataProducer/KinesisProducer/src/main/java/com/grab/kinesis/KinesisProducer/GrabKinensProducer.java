package com.grab.kinesis.KinesisProducer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

public class GrabKinensProducer {

	/**
	 * Timestamp we'll attach to every record
	 */
	private static final String TIMESTAMP = Long.toString(System.currentTimeMillis());

	/**
	 * Change these to try larger or smaller records.
	 */
	private static final int DATA_SIZE = 128;

	/**
	 * Put records for this number of seconds before exiting.
	 */
	private static final int SECONDS_TO_RUN = 5;


	private static final int RECORDS_PER_SECOND = 2000;

	/**
	 * Change this to your stream name.
	 */
	public static final String STREAM_NAME = "test";

	/**
	 * Change this to the region you are using.
	 */
	public static final String REGION = "eu-central-1";

	public static void main(String args[]) throws UnsupportedEncodingException

	{
		try
		{
			
			GrabKinensProducer grabKinesisProvide =new GrabKinensProducer();

			KinesisProducer kinesis =  grabKinesisProvide.getKinesisProducer();
			
			
			AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
	        
			ClientConfiguration config = new ClientConfiguration();
			
			clientBuilder.setRegion(REGION);
			clientBuilder.setCredentials(new DefaultAWSCredentialsProviderChain());
			clientBuilder.setClientConfiguration(config);
			
						        
			AmazonKinesis client = clientBuilder.build();
			
			
			ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
			listStreamsRequest.setLimit(20); 
			ListStreamsResult listStreamsResult = client.listStreams(listStreamsRequest);
			List<String> streamNames = listStreamsResult.getStreamNames();
			
			System.out.println("Strems Found ------------- " + streamNames.toString());
			
			
			System.setProperty("aws.secretKey", "8RfmF4d7BCVy8r4qPWbU6rQJ0db2kL4cBiQ");
			System.setProperty("aws.accessKeyId", "AKIAJW6QNQBLPROZZDGQ");

			System.out.println("Creatiing Connection");

			
			
			
			// Put some records 
			for (int i = 0; i < 100; ++i) {
				ByteBuffer data = ByteBuffer.wrap("myData".getBytes("UTF-8"));
				// doesn't block       
				kinesis.addUserRecord("test", "myParti1tionKey", data); 
			}  
			System.out.println("Producer Ended");
		}
		catch(Exception e)
		{
			e.printStackTrace();	
		}


	}


	public static KinesisProducer getKinesisProducer() {

		KinesisProducerConfiguration config = new KinesisProducerConfiguration();

		
		config.setRegion(REGION);


		config.setCredentialsProvider(new DefaultAWSCredentialsProviderChain());


		config.setMaxConnections(1);


		config.setRequestTimeout(60000);


		config.setRecordMaxBufferedTime(15000);


		KinesisProducer producer = new KinesisProducer(config);

		return producer;
	}
}
