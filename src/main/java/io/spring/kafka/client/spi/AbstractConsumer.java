package io.spring.kafka.client.spi;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractConsumer<K extends Serializable, V extends Serializable> implements ConsumerConfigurer<K, V> {

	public boolean isEnabled() {
		return true;
	}
	
	public String getClientId() {
		return "Client:"+this.getClass().getName();
	}
	
	public Map<String, Object> clientProperties(){
		return new HashMap<String, Object>(0);
	}
}
