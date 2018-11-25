package com.grab.datagenerator.elasticSearch;

public class GeoLocation {

   
    private String name;
    private String location;
    private String geoID ;
    
    


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return String.format("GeoPosition{name='%s', location='%s'}", name, location);
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
}