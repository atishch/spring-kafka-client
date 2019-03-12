package io.spring.kafka.client.utility;

import java.io.Serializable;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.util.SerializationUtils;

public class ObjectDeserializer<T extends Serializable> implements Deserializer<T> {

	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@SuppressWarnings("unchecked")

	public T deserialize(String topic, byte[] objectData) {
		return (objectData == null) ? null : (T) SerializationUtils.deserialize(objectData);
	}

	public void close() {
	}

}


/**
 * $Log$
 *  
 */
