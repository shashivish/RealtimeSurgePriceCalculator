package com.grab.kinesis.KinesisProducer.Helper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.grab.kinesis.KinesisProducer.GrabKinesisWriter;
import com.grab.kinesis.KinesisProducer.Exception.GrabKinesisProducerException;

public class GrabKinesisDataGenerator {

	/**
	 * 
	 * @param typeOfUser
	 * @param streamName
	 * @param driverCsvFilPath
	 * @param kinesisClient
	 * @throws GrabKinesisProducerException
	 * @throws InterruptedException
	 */

	public void postDriverData(String typeOfUser, String streamName , String driverCsvFilPath , AmazonKinesis kinesisClient) throws GrabKinesisProducerException, InterruptedException
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
				grabUserData = typeOfUser+"," + grabUserData;

				System.out.println("Writing Data ");
				System.out.println(grabUserData);

				grabKinesisWriter.sendGrabKinesisRecord(grabUserData, kinesisClient, streamName);

				Thread.sleep(200);
			}

		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

}
