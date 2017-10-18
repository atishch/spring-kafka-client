package com.vmware.kafka.client.consumer;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import com.vmware.kafka.client.ProcessDelegator;

public interface ConsumerConfigurer<K extends Serializable, V extends Serializable>
{

	String getClientId();

	List<String> getTopics();

	boolean isEnabled();

	Properties getConsumerProperties();

	ProcessDelegator<K, V> getProcessDelegator();
	
}
