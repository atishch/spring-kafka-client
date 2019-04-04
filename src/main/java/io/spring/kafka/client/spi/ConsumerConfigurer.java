package io.spring.kafka.client.spi;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.spring.kafka.client.offset.OffsetService;

public interface ConsumerConfigurer<K extends Serializable, V extends Serializable>
{

	boolean isEnabled();
	
	String getClientId();

	List<String> getTopics();


	/**
	 * Apache Kafka Consumer Properties which will be used by Apache Kafka jdk client itself. 
	 * Allowable values can be found {@link org.apache.kafka.clients.consumer.ConsumerConfig}
	 * 
	 * @return
	 */
	Properties getConsumerProperties();

	/**
	 * Service implementation callback
	 * 
	 * @return
	 */
	ProcessDelegator<K, V> getProcessDelegator();

	OffsetService getOffsetService();

	Map<String, Object> clientProperties();
	
}
