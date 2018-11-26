package com.grab.datagenerator.elasticSearch;

public class GeoLocation {

   
    
    private String location;
    private String geoID ;
    private int surgePrice;
    



    @Override
    public String toString() {
        return String.format("GeoPosition{surgePrice='%s', location='%s'}", surgePrice, location);
    }

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getGeoID() {
		return geoID;
	}

	public void setGeoID(String geoID) {
		this.geoID = geoID;
	}

	public int getSurgePrice() {
		return surgePrice;
	}

	public void setSurgePrice(int surgePrice) {
		this.surgePrice = surgePrice;
	}
}