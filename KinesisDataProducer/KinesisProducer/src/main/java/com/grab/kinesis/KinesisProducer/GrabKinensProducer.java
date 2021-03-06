package com.grab.kinesis.KinesisProducer;

import java.io.UnsupportedEncodingException;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.grab.kinesis.KinesisProducer.Config.GrabKinesisProducerConfig;
import com.grab.kinesis.KinesisProducer.Exception.GrabKinesisProducerException;
import com.grab.kinesis.KinesisProducer.Helper.GrabKinesisDataGenerator;

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

	public static void main(String args[]) throws UnsupportedEncodingException, GrabKinesisProducerException
	{

		if(args.length != 4)
		{
			System.err.println("Invalid Number of Argument Provided : Usage GrabKinensProducer <STREAM_NAME>  <TYPE OF USER>  <REGION NAME> <DRIVERCSVPATH>  ");
			System.exit(1);
		}

		String streamName = args[0];
		String regionName = args[1];
		String typeOfUser = args[2];
		String driverCSVPath = args[3];

		//String streamName = "test";
		//String regionName = "eu-central-1";
		//String typeOfUser = "passenger";
		//String driverCSVPath = "/Users/shashi/Documents/Development/Interviews/Grab/data/passanger.csv";

		try
		{

			/**
			 * Create Grab Kinesis Producer Configuration
			 */
			GrabKinesisProducerConfig grabKinesisConfiguration = new GrabKinesisProducerConfig();
			AmazonKinesis amazonKinesis = grabKinesisConfiguration.getKinesisProducer(regionName);


			/**
			 * Generate Data for Producer with Geo location
			 */
			GrabKinesisDataGenerator  grabKinesisGenerator = new GrabKinesisDataGenerator();
			grabKinesisGenerator.postDriverData(typeOfUser, streamName , driverCSVPath , amazonKinesis);


		}
		catch(Exception e)
		{
			e.printStackTrace();	
			throw new GrabKinesisProducerException("Unable to write inot Grab Kinesis Producer");
		}


	}



}
