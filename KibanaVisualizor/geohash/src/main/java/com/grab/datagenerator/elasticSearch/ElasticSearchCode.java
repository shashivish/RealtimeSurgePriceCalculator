package com.grab.datagenerator.elasticSearch;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ElasticSearchCode {

	//The config parameters for the connection
	private static final String HOST = "localhost";
	private static final int PORT_ONE = 9200;
	private static final int PORT_TWO = 9201;
	private static final String SCHEME = "http";

	private static RestHighLevelClient restHighLevelClient;
	private static ObjectMapper objectMapper = new ObjectMapper();

	private static final String INDEX = "gym";
	private static final String TYPE = "gyms";

	/**
	 * Implemented Singleton pattern here
	 * so that there is just one connection at a time.
	 * @return RestHighLevelClient
	 */
	private static synchronized RestHighLevelClient makeConnection() {

		if(restHighLevelClient == null) {
			restHighLevelClient = new RestHighLevelClient(
					RestClient.builder(
							new HttpHost(HOST, PORT_ONE, SCHEME),
							new HttpHost(HOST, PORT_TWO, SCHEME)));
		}

		return restHighLevelClient;
	}

	private static synchronized void closeConnection() throws IOException {
		restHighLevelClient.close();
		restHighLevelClient = null;
	}

	private static GeoLocation insertGeoPosition(GeoLocation geoLocation){
		geoLocation.setGeoID(UUID.randomUUID().toString());

		Map<String, Object> dataMap = new HashMap<String, Object>();

		dataMap.put("name", geoLocation.getName());
		dataMap.put("location", geoLocation.getLocation());

		IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, geoLocation.getGeoID())
				.source(dataMap);
		try {
			IndexResponse response = restHighLevelClient.index(indexRequest);
		} catch(ElasticsearchException e) {
			e.getDetailedMessage();
		} catch (java.io.IOException ex){
			ex.getLocalizedMessage();
		}
		return geoLocation;
	}

	private static GeoLocation getPersonById(String id){
		GetRequest getPersonRequest = new GetRequest(INDEX, TYPE, id);
		GetResponse getResponse = null;
		try {
			getResponse = restHighLevelClient.get(getPersonRequest);
		} catch (java.io.IOException e){
			e.getLocalizedMessage();
		}
		return getResponse != null ?
				objectMapper.convertValue(getResponse.getSourceAsMap(), GeoLocation.class) : null;
	}

	private static GeoLocation updatePersonById(String id, GeoLocation person){
		UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, id)
				.fetchSource(true);    // Fetch Object after its update
		try {
			String personJson = objectMapper.writeValueAsString(person);
			updateRequest.doc(personJson, XContentType.JSON);
			UpdateResponse updateResponse = restHighLevelClient.update(updateRequest);
			return objectMapper.convertValue(updateResponse.getGetResult().sourceAsMap(), GeoLocation.class);
		}catch (JsonProcessingException e){
			e.getMessage();
		} catch (java.io.IOException e){
			e.getLocalizedMessage();
		}
		System.out.println("Unable to update person");
		return null;
	}

	private static void deletePersonById(String id) {
		DeleteRequest deleteRequest = new DeleteRequest(INDEX, TYPE, id);
		try {
			DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest);
		} catch (java.io.IOException e){
			e.getLocalizedMessage();
		}
	}

	public void pushDatatoElasticSearch (String geoHash) 
	{

		makeConnection();

		System.out.println("Inserting a new Person with name Shubham...");
		GeoLocation geolocation = new GeoLocation();
		geolocation.setName("Shubham");
		geolocation.setLocation(geoHash);

		geolocation = insertGeoPosition(geolocation);
		System.out.println("Geo Inserted inserted --> " + geolocation);

		/* System.out.println("Changing name to `Shubham Aggarwal`...");
        person.setName("Shubham Aggarwal");
        updatePersonById(person.getPersonId(), person);
        System.out.println("Person updated  --> " + person);

        System.out.println("Getting Shubham...");
        Person personFromDB = getPersonById(person.getPersonId());
        System.out.println("Person from DB  --> " + personFromDB);

        System.out.println("Deleting Shubham...");
        deletePersonById(personFromDB.getPersonId());
        System.out.println("Person Deleted");
		 */
		try {
			closeConnection();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
