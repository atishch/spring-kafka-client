package com.vmware.kafka.client.consumer;

import java.io.Serializable;

public abstract class ConsumerAbstractConfig<K extends Serializable, V extends Serializable> implements ConsumerConfigurer<K, V> {

	@Override
	public boolean isEnabled() {
		return true;
	}
	
	@Override
	public String getClientId() {
		return "Client:"+this.getClass().getName();
	}
}
