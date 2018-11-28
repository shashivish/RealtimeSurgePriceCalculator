package com.grab.kinesis.KinesisProducer;

import java.nio.ByteBuffer;
import java.util.Random;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.grab.kinesis.KinesisProducer.Exception.GrabKinesisProducerException;

public class GrabKinesisWriter {



	private final Random random = new Random();


	public  void validateStream(AmazonKinesis kinesisClient, String streamName) {
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



	public  void sendGrabKinesisRecord(String grabUserData, AmazonKinesis kinesisClient,
			String streamName) throws GrabKinesisProducerException {


		byte[] bytes = grabUserData.getBytes();

		// The bytes could be null if there is an issue with the JSON serialization by the Jackson JSON library.
		if (bytes == null) {
			System.out.println( "Data is empty");
			return;
		}

		int randomInteger = random.nextInt();

		PutRecordRequest putRecord = new PutRecordRequest();
		putRecord.setStreamName(streamName);
		putRecord.setPartitionKey( String.valueOf(randomInteger) );
		putRecord.setData(ByteBuffer.wrap(bytes));

		try {
			kinesisClient.putRecord(putRecord);
			System.out.println( "Data is written to Kinesis Stream");
		} catch (AmazonClientException ex) {
			ex.printStackTrace();
			System.out.println("Failed to write in Kinesis Producer");
			throw new GrabKinesisProducerException("Failed to write in Kinesis Producer");

		}
	}


}
