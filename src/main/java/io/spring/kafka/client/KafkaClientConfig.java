package io.spring.kafka.client;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import io.spring.kafka.client.boot.ConsumerAutoRunner;

public class KafkaClientConfig {
	
	//This Bean is required to Enable the Consumer Auto Configuration
	@Bean
	ConsumerAutoRunner consumerAutoRunner(){
		return new ConsumerAutoRunner();
		
	}

}
