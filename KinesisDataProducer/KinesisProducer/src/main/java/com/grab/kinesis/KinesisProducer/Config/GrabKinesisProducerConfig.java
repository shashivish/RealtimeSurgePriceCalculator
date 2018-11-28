package com.grab.kinesis.KinesisProducer.Config;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.grab.kinesis.KinesisProducer.Exception.GrabKinesisProducerException;

/**
 * Get Kinesis Producer Config
 * @author shashi
 *
 */
public class GrabKinesisProducerConfig {



	public AmazonKinesis getKinesisProducer(String regionName ) throws GrabKinesisProducerException {

		AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();

		if(regionName.equals(""))
		{
			throw new GrabKinesisProducerException("Region Name is not provided");

		}
		clientBuilder.setRegion(regionName);

		try {
			clientBuilder.setCredentials(CredentialUtils.getCredentialsProvider());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		clientBuilder.setClientConfiguration(ConfigurationUtils.getClientConfigWithUserAgent());

		AmazonKinesis kinesisClient = clientBuilder.build();
		return kinesisClient;
	}
}
