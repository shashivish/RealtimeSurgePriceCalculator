package com.grab.datagenerator.geohash;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

import com.grab.datagenerator.elasticSearch.ElasticSearchCode;

import ch.hsr.geohash.*;

/**
 * Hello world!
 *
 */
public class GeoHashGenerator 
{

	ElasticSearchCode elst = new ElasticSearchCode();

	public static void main( String[] args )
	{
		String csvPathFile="/Users/shashi/Documents/Development/Interviews/Grab/data/test.csv";

		//GeoHashGenerator 


		GeoHashGenerator geoHashgenerator = new GeoHashGenerator();
		geoHashgenerator.readCSV(csvPathFile);

		//String geohash = geoHashgenerator.getLocation(1.35340459, 103.81794179, 1);
		//System.out.println("Got Geo Hash  " + geohash);
	}

	public void readCSV(String csvFilPath )
	{
		BufferedReader br = null;

		String csvFile = csvFilPath;
		String line = "";
		String cvsSplitBy = ",";
		int i=0;

		try 
		{

			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) 
			{


				//if (i == 1000)
					//break;

				// use comma as separator
				String[] country = line.split(cvsSplitBy);

				System.out.println("Longitude : " + country[6] + "  Lattitude : " + country[7] + "");

				String longitude = country[5];
				String lattitude = country[6];

				if (longitude.equalsIgnoreCase("pickup_latitude"))
				{
					// This is to avoid header 
					System.out.println("Came  here ");
				}
				else
				{

					System.out.println("Else here");
					String obtainedGeoHash = geGeoHash(Double.parseDouble(longitude) ,Double.parseDouble(lattitude));

					System.out.println( "Obtained Geo Hash " + obtainedGeoHash);
					elst.pushDatatoElasticSearch(obtainedGeoHash);
				}

				i = i+1;
				System.out.println( "------------- Record Number " + i);


			}

		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String  getLocation(double x0, double y0, int radius) {
		Random random = new Random();
		// System.out.println(random);

		// Convert radius from meters to degrees
		double radiusInDegrees = radius / 111000f;

		double u = random.nextDouble();
		double v = random.nextDouble();

		System.out.println("" + u + "--" + v);
		double w = radiusInDegrees * Math.sqrt(u);
		double t = 2 * Math.PI * v;
		double x = w * Math.cos(t);
		double y = w * Math.sin(t);

		// Adjust the x-coordinate for the shrinking of the east-west distances
		double new_x = x / Math.cos(Math.toRadians(y0));


		double foundLongitude = new_x + x0;
		double foundLatitude = y + y0;
		System.out.println("Longitude: " + foundLongitude + "  Latitude: " + foundLatitude );

		return geGeoHash(foundLongitude,foundLatitude);

	}

	static String geGeoHash(double logitude , double lattitude )
	{

		
		String geohashString = GeoHash.withCharacterPrecision(logitude , lattitude, 6).toBase32();
		return geohashString;
		

	}
}
