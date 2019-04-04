package io.spring.kafka.client.spi;

import java.io.Serializable;


public interface ProcessDelegator<K extends Serializable, V extends Serializable> {

	public boolean process(K key, V value) throws Exception;
	
}
