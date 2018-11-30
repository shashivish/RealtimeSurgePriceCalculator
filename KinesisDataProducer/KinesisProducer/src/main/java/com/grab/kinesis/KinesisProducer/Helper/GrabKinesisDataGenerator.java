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

	final static String DRIVER="driver";
	final static String PASSANGER="passanger";

	GrabKinesisWriter grabKinesisWriter ;
	GeoHashGenerator geoHashGenerator ;


	/**
	 * 
	 * @param typeOfUser
	 * @param streamName
	 * @param driverCsvFilPath
	 * @param kinesisClient
	 * @throws GrabKinesisProducerException
	 * @throws InterruptedException
	 */

	@SuppressWarnings("resource")
	public void postDriverData(String typeOfUser, String streamName , String driverCsvFilPath , AmazonKinesis kinesisClient) throws GrabKinesisProducerException, InterruptedException
	{
		BufferedReader br = null;

		String csvFile = driverCsvFilPath;
		String grabUserData = "";
		String cvsSplitBy = ",";
		int i=0;
		String grabUserDataToPost="";

		grabKinesisWriter = new GrabKinesisWriter();
		geoHashGenerator = new GeoHashGenerator();

		/**
		 * Validate Stream if it is valid
		 */
		//grabKinesisWriter.validateStream(kinesisClient, streamName);

		try 
		{

			br = new BufferedReader(new FileReader(csvFile));
			while ((grabUserData = br.readLine()) != null) 
			{

				if (grabUserData.contains("pickup_longitude"))
				{
					System.out.println("Skipping Header Row");
					continue;
				}

				String[] userDataElement = grabUserData.split(",");

				/**
				 * Send if Type is Driver
				 */
				if(DRIVER.equalsIgnoreCase(typeOfUser))
				{

					String geoHash = geoHashGenerator.geGeoHash(Double.parseDouble(userDataElement[4]), Double.parseDouble(userDataElement[5]));
					System.out.println("Generated GeoHash " + geoHash);

					grabUserDataToPost = typeOfUser+"," + userDataElement[2]+","+geoHash;
				}
				else
				{
					/**
					 * Send if Type if Passanger
					 */
					if(PASSANGER.equalsIgnoreCase(typeOfUser))
					{
						String geoHash = geoHashGenerator.geGeoHash(Double.parseDouble(userDataElement[5]), Double.parseDouble(userDataElement[6]));
						System.out.println("Generated GeoHash " + geoHash);
						grabUserDataToPost = typeOfUser+"," + userDataElement[2]+","+userDataElement[5]+","+userDataElement[6];
					}
				}

				System.out.println("Writing Data ");
				System.out.println(grabUserDataToPost);

				//	grabKinesisWriter.sendGrabKinesisRecord(grabUserDataToPost, kinesisClient, streamName);

				Thread.sleep(200);
			}

		}
		catch (IOException e) {
			e.printStackTrace();
			throw new GrabKinesisProducerException("Unable to post message to Kinessi " + e.getMessage());
		}
	}

}
