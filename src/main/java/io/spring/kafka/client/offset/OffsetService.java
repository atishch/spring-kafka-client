package io.spring.kafka.client.offset;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;

public interface OffsetService {

	void commitOffset(Map<TopicPartition, Long> partitionToOffsetMap);

	Long getOffset(TopicPartition topicPart);

}