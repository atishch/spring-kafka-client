package io.spring.kafka.client.event;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public interface KafkaConsumerAware {
	void setKafkaConsumer(KafkaConsumer<?, ?> consumer);
}
