package com.grab.kinesis.KinesisProducer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

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
    
	public static void main(String args[]) throws UnsupportedEncodingException

	{
		try
		{
			System.out.println("Creatiing Connection");
			KinesisProducer kinesis = new KinesisProducer();  
			// Put some records 
			for (int i = 0; i < 100; ++i) {
				ByteBuffer data = ByteBuffer.wrap("myData".getBytes("UTF-8"));
				// doesn't block       
				kinesis.addUserRecord("test", "myParti1tionKey", data); 
			}  
			System.out.println("Producer Ended");
		}
		catch(Exception e)
		{
			e.printStackTrace();	
		}
		

	}
	
	


 
    public static KinesisProducer getKinesisProducer() {
        
        KinesisProducerConfiguration config = new KinesisProducerConfiguration();
        
       
        config.setRegion(REGION);
        
       
        config.setCredentialsProvider(new DefaultAWSCredentialsProviderChain());
        
        
        config.setMaxConnections(1);
        
       
        config.setRequestTimeout(60000);
        
       
        config.setRecordMaxBufferedTime(15000);

       
        KinesisProducer producer = new KinesisProducer(config);
        
        return producer;
    }
}
