package io.spring.kafka.client.consumer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.spring.kafka.client.ConsumerConstants;
import io.spring.kafka.client.event.KafkaConsumerAware;
import io.spring.kafka.client.offset.OffsetService;
import io.spring.kafka.client.spi.ConsumerConfigurer;
import io.spring.kafka.client.spi.ProcessDelegator;
import io.spring.kafka.client.utility.KafkaUtils;



public class DynamicConsumer<K extends Serializable, V extends Serializable> implements Runnable {
	
	
	//Necessary dependencies
	private OffsetService offsetService;
	
	private ProcessDelegator<K,V> processDelegator; //This is the Optional process which Consumer will call
		
	private List<String> topics;
	
	private static final Logger logger = LoggerFactory.getLogger(DynamicConsumer.class);
	
	//Global Default Settings
	private long kafkaPollTimeoutMs;
	private long waitHeartbeatMs;
	private long processTerminationTimeoutMs;
	
	//Local Field Variable
	protected KafkaConsumer<K, V> consumer;
	private final String clientId;
	private String threadName;
	private String printTopics;
	
	private AtomicBoolean closed = new AtomicBoolean();
	private CountDownLatch shutdownLatch = new CountDownLatch(1);
	
	public DynamicConsumer(ConsumerConfigurer<K,V> config){
		this.clientId = config.getClientId();
		this.topics = config.getTopics();
		Properties consumerProps = config.getConsumerProperties();
		if(consumerProps == null) {
			consumerProps = new Properties();
		}
		if(this.clientId != null){
			consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, this.clientId);
		}
		this.consumer = new KafkaConsumer<K, V>(consumerProps);
		this.processDelegator = config.getProcessDelegator();
		this.offsetService = initOffsetService(config);
		init(config.clientProperties());
	}
	
	private OffsetService initOffsetService(ConsumerConfigurer<K,V> config) {
		OffsetService offsetSevice = config.getOffsetService();
		if(offsetSevice instanceof KafkaConsumerAware) {
			((KafkaConsumerAware)offsetSevice).setKafkaConsumer(this.consumer);
		}
		return offsetSevice;
	}

	public void init(Map<String, Object> overrideSettings) {
		printTopics = KafkaUtils.printTopics(this.topics);
		
		Map<String, Object> defaulSettings = ConsumerConstants.defaultSystemValues();
		if(overrideSettings!=null && !overrideSettings.isEmpty()){
			defaulSettings.putAll(overrideSettings);
		}
		this.kafkaPollTimeoutMs = Long.parseLong(defaulSettings.get(ConsumerConstants.SYSTEM_CONSUMER_POLL_TIMEOUT_MS).toString());
		this.processTerminationTimeoutMs = Long.parseLong(defaulSettings.get(ConsumerConstants.SYSTEM_CONSUMER_PROCESS_TERMINATION_TIMEOUT_MS).toString());
		this.waitHeartbeatMs  = Long.parseLong(defaulSettings.get(ConsumerConstants.SYSTEM_CONSUMER_WAIT_HEARTBEAT_MS).toString());
		
		this.threadName = "Processor_"+printTopics.trim().replaceAll(" ","-");
		
	}

	public void run() {
	
		logger.info("Starting consumer : {} for topics: {}", clientId,printTopics);

		ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(threadName).build());
		final Map<TopicPartition, Long> partitionToUncommittedOffsetMap = new ConcurrentHashMap<TopicPartition, Long>();
		final List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
		
		ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {

			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				if(!futures.isEmpty())
					futures.get(0).cancel(true);
				
				logger.info("ClientID : {},Topic: {} , Revoked topicPartitions : {}", clientId,printTopics, partitions);
				offsetService.commitOffset(partitionToUncommittedOffsetMap);
			}

			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				for (TopicPartition tp : partitions) {
					long startOffset = offsetService.getOffset(tp); 
					logger.info("ClientID : {},Topic: {} , Assigned topicPartion : {} offset : {}", clientId,printTopics, tp, startOffset);
					
					if(startOffset >= 0){
						consumer.seek(tp, startOffset);
					}
					
				}
			}
		};
		
		consumer.subscribe(topics, listener);
		logger.info("Started to process records for consumer : {} Topic: {} ", clientId,printTopics);
		
		while(!closed.get()) {
			
			ConsumerRecords<K, V> records = consumer.poll(kafkaPollTimeoutMs);
			
			if(records.isEmpty()) {
				logger.info("ClientID : {},Topic: {} , Found no records", clientId,printTopics);
				continue;
			}
			
			/**
			 * After receiving the records, pause all the partitions and do heart-beat manually
			 * to avoid the consumer instance gets kicked-out from the group by the consumer coordinator
			 * due to the delay in the processing of messages
			 */
			consumer.pause(consumer.assignment()); // after doing this operation consumer.poll will work only has heartbeat.
			Future<Boolean> future = executor.submit(new ProcessConsumerRecords(records, partitionToUncommittedOffsetMap));
			futures.add(future);
			
			Boolean isCompleted = false;
			while(!isCompleted && !closed.get()) {
				try	{
					isCompleted = future.get(waitHeartbeatMs , TimeUnit.MILLISECONDS); // wait up-to heart-beat interval
				} catch (TimeoutException e) {
					logger.debug("ClientID : {},Topic: {} , heartbeats the coordinator", clientId,printTopics);
					consumer.poll(0); // does heart-beat
					//Commit time to time as some might have processed the Message
					offsetService.commitOffset(partitionToUncommittedOffsetMap); //NO SONAR 
				} catch (CancellationException e) {
					logger.debug("ClientID : {},Topic: {} , ConsumeRecords Job got cancelled", clientId,printTopics);
					break;
				} catch (ExecutionException | InterruptedException e) {
					logger.error("ClientID : {},Topic: {} , Error while consuming records", clientId,printTopics, e);
					break;
				}
			}
			futures.remove(future);
			consumer.resume(consumer.assignment()); // Consumer.poll will fetch the messages.
			offsetService.commitOffset(partitionToUncommittedOffsetMap);
		}
		
		try {
			executor.shutdown();
			int count=1;
			while(!executor.awaitTermination(processTerminationTimeoutMs, TimeUnit.MILLISECONDS)){
				logger.info("ClientID : {},Topic: {} , Stopping.. {}", clientId,printTopics,count);
				count++;
			}
		} catch (InterruptedException e) {
			logger.error("ClientID : {},Topic: {} , Error while exiting the consumer", clientId,printTopics, e);
		}
		consumer.close();
		shutdownLatch.countDown();
		logger.info("ClientID : {},Topic: {} , consumer exited", clientId,printTopics);
	}

	public void close() {
		try {
			closed.set(true);
			shutdownLatch.await();
		} catch (InterruptedException e) {
			logger.error("Error", e);
		}
	}
	
	
	
	private class ProcessConsumerRecords implements Callable<Boolean> {
		
		ConsumerRecords<K, V> records;
		Map<TopicPartition, Long> partitionToUncommittedOffsetMap;
		
		public ProcessConsumerRecords(ConsumerRecords<K, V> records, Map<TopicPartition, Long> partitionToUncommittedOffsetMap) {
			this.records = records;
			this.partitionToUncommittedOffsetMap = partitionToUncommittedOffsetMap;
		}
		
		@Override
		public Boolean call() {
			logger.info("ClientID : {},Topic: {} , Number of records received : {}", clientId,printTopics, records.count());
			try {
				for(ConsumerRecord<K, V> record : records) {
					TopicPartition tp = new TopicPartition(record.topic(), record.partition());
					logger.info("ClientID : {},Topic: {} , Record received topicPartition : {}, offset : {}", clientId,printTopics, tp, record.offset());
					boolean shouldCommit =true;
					if(processDelegator!=null){
						shouldCommit = false;
						shouldCommit = processDelegator.process(record.key(), record.value());
					}
					if(shouldCommit){
						partitionToUncommittedOffsetMap.put(tp, record.offset());
					}
				}
			} catch (InterruptedException e) {
				logger.info("ClientID : {},Topic: {} , Record consumption interrupted!", clientId,printTopics);
			} catch (Exception e) {
				logger.error("Error while consuming", e);
			}
			return true;
		}		
	}
	
	
}