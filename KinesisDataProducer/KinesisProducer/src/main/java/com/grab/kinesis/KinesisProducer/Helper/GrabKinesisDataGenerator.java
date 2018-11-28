package com.grab.kinesis.KinesisProducer.Helper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.grab.kinesis.KinesisProducer.GrabKinesisWriter;
import com.grab.kinesis.KinesisProducer.Exception.GrabKinesisProducerException;

public class GrabKinesisDataGenerator {

	static String DRIVER="driver";
	static String PASSANGER="pasanger";
	public void postDriverData(String streamName , String driverCsvFilPath , AmazonKinesis kinesisClient) throws GrabKinesisProducerException
	{
		BufferedReader br = null;

		String csvFile = driverCsvFilPath;
		String grabUserData = "";
		String cvsSplitBy = ",";
		int i=0;


		GrabKinesisWriter grabKinesisWriter = new GrabKinesisWriter();

		/**
		 * Validate Stream if it is valid
		 */
		grabKinesisWriter.validateStream(kinesisClient, streamName);

		try 
		{

			br = new BufferedReader(new FileReader(csvFile));
			while ((grabUserData = br.readLine()) != null) 
			{
				grabUserData = DRIVER+"," + grabUserData;
				grabKinesisWriter.sendGrabKinesisRecord(grabUserData, kinesisClient, streamName);
			}

		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

}