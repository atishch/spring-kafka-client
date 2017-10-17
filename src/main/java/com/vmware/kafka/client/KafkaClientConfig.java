package com.vmware.kafka.client;


import org.springframework.context.annotation.Bean;


import com.vmware.kafka.client.boot.ConsumerAutoRunner;

//@Configuration
//Manually define all the needed Bean
//@ComponentScan(basePackages = { "com.vmware.kafka.client" })
public class KafkaClientConfig {
	
	
	//This Bean is required to Enable the Consumer Auto Configuration
	@SuppressWarnings("rawtypes")
	@Bean
	ConsumerAutoRunner consumerAutoRunner(){
		return new ConsumerAutoRunner();
		
	}

}
