package io.spring.kafka.client;

import java.io.Serializable;

public abstract class ConsumerAbstractConfig<K extends Serializable, V extends Serializable> implements ConsumerConfigurer<K, V> {

	public boolean isEnabled() {
		return true;
	}
	
	public String getClientId() {
		return "Client:"+this.getClass().getName();
	}
}
