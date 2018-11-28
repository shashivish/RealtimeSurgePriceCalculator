package com.grab.kinesis.KinesisProducer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

public class GrabKinesisWriter {



	private static void validateStream(AmazonKinesis kinesisClient, String streamName) {
		try {
			DescribeStreamResult result = kinesisClient.describeStream(streamName);
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

	}
}
