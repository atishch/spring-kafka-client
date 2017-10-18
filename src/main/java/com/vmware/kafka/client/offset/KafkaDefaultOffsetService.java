package com.vmware.kafka.client.offset;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaDefaultOffsetService implements OffsetService 
{
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaDefaultOffsetService.class);
			
	private Consumer<?, ?> consumer;
	

	public KafkaDefaultOffsetService(KafkaConsumer<?, ?> consumer) {
		this.consumer = consumer;
	}

	public void setConsumer(Consumer<?, ?> consumer) {
		this.consumer = consumer;
		
	}
	
	/* (non-Javadoc)
	 * @see com.vmware.kafka.client.offset.OffsetService#commitOffset(java.util.Map)
	 */
	@Override
	public void commitOffset(Map<TopicPartition, Long> partitionToOffsetMap) {
		if(this.consumer != null){
			if(!partitionToOffsetMap.isEmpty()) {
				Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = new HashMap<>();
				for(Entry<TopicPartition, Long> e : partitionToOffsetMap.entrySet()) {
					partitionToMetadataMap.put(e.getKey(), new OffsetAndMetadata(e.getValue() + 1));
				}
				
				logger.info("Committing the offsets : {}", partitionToMetadataMap);
				consumer.commitSync(partitionToMetadataMap);
				partitionToOffsetMap.clear();
			}
		}else{
			logger.error("Configure a proper OffsetManager");
		}	
	}
	
	
	/* (non-Javadoc)
	 * @see com.vmware.kafka.client.offset.OffsetService#getOffset(org.apache.kafka.common.TopicPartition)
	 */
	@Override
	public Long getOffset(TopicPartition topicPart) {
		if(this.consumer != null){
			//User Kafka Default consumer Sync
			OffsetAndMetadata metaAndOffset = consumer.committed(topicPart);
			long startOffset = metaAndOffset != null ? metaAndOffset.offset() : -1L;
			return startOffset;
		}else{
			logger.error("Configure a proper OffsetManager");
		}
		return -1L;
	}
	

}
