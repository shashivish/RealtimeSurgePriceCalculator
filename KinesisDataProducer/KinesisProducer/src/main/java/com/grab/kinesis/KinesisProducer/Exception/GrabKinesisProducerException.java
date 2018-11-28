package com.grab.kinesis.KinesisProducer.Exception;


/**
 *  Custom Exception Handling
 * @author shashi
 *
 */

public class GrabKinesisProducerException extends Exception{

	private static final long serialVersionUID = 1L;

	public GrabKinesisProducerException(String message) {
		super(message);
	}

}
