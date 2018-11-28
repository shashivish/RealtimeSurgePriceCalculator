package com.grab.kinesis.KinesisProducer;

import java.nio.ByteBuffer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

public class GrabKinesisWriter {



	private static void validateStream(AmazonKinesis kinesisClient, String streamName) {
		try {
			DescribeStreamResult result = kinesisClient.describeStream(streamName);
			System.out.println( "Stream Name " + streamName + "  Status  : " + result.getStreamDescription().getStreamStatus());
			if(!"ACTIVE".equals(result.getStreamDescription().getStreamStatus())) {
				System.err.println("Stream " + streamName + " is not active. Please wait a few moments and try again.");
				System.exit(1);
			}
		} catch (ResourceNotFoundException e) {
			System.err.println("Stream " + streamName + " does not exist. Please create it in the console.");
			System.err.println(e);
			System.exit(1);
		} catch (Exception e) {
			System.err.println("Error found while describing the stream " + streamName);
			System.err.println(e);
			System.exit(1);
		}
	}



	private static void sendGrabKinesisRecord(String grabUserData, AmazonKinesis kinesisClient,
			String streamName) {


		//		for (int i = 0; i < 100; ++i) {
		//			ByteBuffer data = ByteBuffer.wrap("myData".getBytes("UTF-8"));
		//			// doesn't block       
		//			kinesisClient.addUserRecord("test", "myParti1tionKey", data); 
		//		}  


		byte[] bytes = grabUserData.getBytes();

		// The bytes could be null if there is an issue with the JSON serialization by the Jackson JSON library.
		if (bytes == null) {
			System.out.println( "Data is empty");
			return;
		}

		//   LOG.info("Putting trade: " + trade.toString());
		PutRecordRequest putRecord = new PutRecordRequest();
		putRecord.setStreamName(streamName);
		// We use the ticker symbol as the partition key, as explained in the tutorial.
		putRecord.setPartitionKey("testKey");
		putRecord.setData(ByteBuffer.wrap(bytes));

		try {
			kinesisClient.putRecord(putRecord);
			System.out.println( "Data is written to Kinesis Stream");
		} catch (AmazonClientException ex) {
			ex.printStackTrace();
			System.out.println("Failed to write in Kinesis Producer");


		}
	}

	public void initKinesis(String streamName , String regionName)
	{

		AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();

		clientBuilder.setRegion(regionName);

		try {
			clientBuilder.setCredentials(CredentialUtils.getCredentialsProvider());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		clientBuilder.setClientConfiguration(ConfigurationUtils.getClientConfigWithUserAgent());

		AmazonKinesis kinesisClient = clientBuilder.build();



		// Validate that the stream exists and is active
		validateStream(kinesisClient, streamName);

		for(int i =0; i < 10000000 ; i++ )
		{
			System.out.println("Record Number " + i+1);
			sendGrabKinesisRecord("Hello Grab" ,kinesisClient ,streamName);
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}



	}
}
