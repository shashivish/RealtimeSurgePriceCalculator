package com.grab.kinesis.KinesisProducer.Helper;

import ch.hsr.geohash.GeoHash;

public class GeoHashGenerator {


	/**
	 * Generate GeoHash for given langitude and lattitude.
	 * @param logitude
	 * @param lattitude
	 * @return
	 */
	public String geGeoHash(double logitude , double lattitude )
	{
		String geohashString = GeoHash.withCharacterPrecision(logitude , lattitude, 6).toBase32();
		return geohashString;


	}

}
