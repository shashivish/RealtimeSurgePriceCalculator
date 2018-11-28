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
		String streamName = args[0];
		String regionName = args[1];
		String driverCSVPath = args[2];


		if(args.length != 3)
		{
			System.err.println("Invalid Number of Argument Provided : Usage GrabKinensProducer <STREAM_NAME>  <REGION NAME> <DRIVERCSVPATH>  <PASSANGERCVSPATH> ");
			System.exit(1);
		}

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
			grabKinesisGenerator.postDriverData( streamName , driverCSVPath , amazonKinesis);


		}
		catch(Exception e)
		{
			e.printStackTrace();	
			throw new GrabKinesisProducerException("Unable to write inot Grab Kinesis Producer");
		}


	}



}
