package io.spring.kafka.client.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public interface KafkaConsumerAware {
	void setKafkaConsumer(KafkaConsumer<?, ?> consumer);
}
