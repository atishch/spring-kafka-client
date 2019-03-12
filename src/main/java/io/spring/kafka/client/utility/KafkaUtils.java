package io.spring.kafka.client.utility;

import java.util.List;

public class KafkaUtils {

	public static String printTopics(List<String> topics){
		if(topics==null || topics.isEmpty()){
			return "";
		}
		StringBuilder buff = new StringBuilder();
		for(String topic: topics){
			buff.append(" ").append(topic);
		}
		return buff.toString();
	}
}
