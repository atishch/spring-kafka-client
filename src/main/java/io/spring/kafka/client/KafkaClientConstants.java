package io.spring.kafka.client;

import java.util.HashMap;
import java.util.Map;

public class KafkaClientConstants 
{
	
	public static String SYSTEM_CONSUMER_POLL_TIMEOUT_MS;
	
	public static String SYSTEM_CONSUMER_WAIT_HEARTBEAT_MS;
	
	public static String SYSTEM_CONSUMER_PROCESS_TERMINATION_TIMEOUT_MS;
	
	
	public static Map<String,Object> defaultSystemValues(){
		Map<String,Object> valueMap = new HashMap<String, Object>();
		valueMap.put(KafkaClientConstants.SYSTEM_CONSUMER_POLL_TIMEOUT_MS, 5000L);
		valueMap.put(KafkaClientConstants.SYSTEM_CONSUMER_WAIT_HEARTBEAT_MS, 5000L);
		valueMap.put(KafkaClientConstants.SYSTEM_CONSUMER_PROCESS_TERMINATION_TIMEOUT_MS, 5000L);
		return valueMap;
	}
	
}
