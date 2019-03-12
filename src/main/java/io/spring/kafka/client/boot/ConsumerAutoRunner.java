package io.spring.kafka.client.boot;

import java.io.Serializable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.spring.kafka.client.ConsumerConfigurer;
import io.spring.kafka.client.consumer.DynamicConsumer;
import io.spring.kafka.client.utility.KafkaUtils;


public class ConsumerAutoRunner<K extends Serializable, V extends Serializable> implements SmartLifecycle{

	private static final Logger logger = LoggerFactory.getLogger(ConsumerAutoRunner.class);
			
	@Autowired
	List<ConsumerConfigurer<K,V>> consumerConfiguations;
	
	//Field Required for Functional Logic
	private boolean needConfig = false;
	
	Map<String,DynamicConsumer<K, V>> consumers;
	
	//Spring Lifecycle methods 
	//DO NOT REMOVE
	private boolean autoStartup = true;

	private int phase = Integer.MAX_VALUE;

	private volatile boolean running = false;

	private Object lifecycleMonitor = new Object();

	@Value("${kafka.consumers.shutdownTimeoutMs:10000}")
	private long shutdownTimeoutMillis;
	
	Map<String,ExecutorService> topicExecutors;
	

	@PostConstruct
	public void init(){
		if(consumerConfiguations!=null && !consumerConfiguations.isEmpty()){
			consumers = new HashMap<String,DynamicConsumer<K, V>>(consumerConfiguations.size());
			topicExecutors = new ConcurrentHashMap<String, ExecutorService>(consumerConfiguations.size());
			for(ConsumerConfigurer<K,V> consumerConfig : consumerConfiguations){
				String clientId = consumerConfig.getClientId();
				ExecutorService executor = Executors.newFixedThreadPool(1,new ThreadFactoryBuilder().setNameFormat("Consumer_"+clientId).build());
				topicExecutors.put(clientId,executor);
			}
			needConfig = true;
		}else{
			logger.warn("Could not find any Consumers. Implement interface '"+ConsumerConfigurer.class.getName()+"' to create Consumers");
		}
	}
	
	public final void start() {
		synchronized (this.lifecycleMonitor ) {
			doStart();
		}
	}

	protected void doStart() {
		if (isRunning()) {
			return;
		}
		if(needConfig){
			// Start consumers one by one after 10 seconds
			for(ConsumerConfigurer<K,V> config:consumerConfiguations){
				Boolean enabled = config.isEnabled();
				if(enabled == null || enabled.booleanValue() == false){
					logger.warn("Consumer '{}' is not enabled.",config.getClass().getName());
					continue;
				}
				List<String> topics = config.getTopics();
				if(topics == null || topics.isEmpty()){
					logger.warn("Consumer '{}' has not configured to to Listen to any Topic.",config.getClass().getName());
					continue;
				}
				String clientId = config.getClientId();
				if(clientId == null || clientId.isEmpty()){
					logger.warn("Consumer '{}' has not configured with clientId.",config.getClass().getName());
					continue;
				}
				ExecutorService executor = topicExecutors.get(clientId);
				startConsumerInPool(executor,config,clientId);
				
			}
			
		
		}
		this.running = true;
	}

	private void startConsumerInPool(ExecutorService executor,ConsumerConfigurer<K,V> config,String clientId) {
		DynamicConsumer<K, V> consumer = new DynamicConsumer<K, V>(config);
		consumers.put(clientId,consumer);
		executor.submit(consumer);
		logger.info("Started process for Consumer: {} for Topics:{}",config.getClientId(),KafkaUtils.printTopics(config.getTopics()));
		try {
			Thread.sleep(TimeUnit.SECONDS.toMillis(10));
		} catch (InterruptedException e) {
			//e.printStackTrace();
		}
	}
	
	public final void stop() {
		final CountDownLatch latch = new CountDownLatch(1);
		stop(new Runnable() {
			public void run() {
				latch.countDown();
			}
		});
		try {
			latch.await(shutdownTimeoutMillis , TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void stop(Runnable callback) {
		synchronized (this.lifecycleMonitor) {
			doStop(callback);
		}
	}

	
	protected void doStop(Runnable callback) {
		if(!running){
			return;
		}
		for(String clientId:topicExecutors.keySet()){
			logger.info("Shutting down Consumer with ClientID {}",clientId);
			ExecutorService executor = topicExecutors.get(clientId);
			shutdownClient(clientId, executor);
		}
		this.running = false;
		new Thread(callback).start();
	}
	
	public void shutdownClient(String clientId,ExecutorService clientExecutor){
		DynamicConsumer<K, V> consumer = consumers.get(clientId);
		if(consumer != null){
			consumer.close();
			try {
				Thread.sleep(TimeUnit.SECONDS.toMillis(2));
			} catch (InterruptedException e) {
				logger.error("Error stopping Consumer with ClientID: {}",clientId,e);
			}
		}
		clientExecutor.shutdown(); // Disable new tasks from being submitted
		   try {
		     // Wait a while for existing tasks to terminate
		     if (!clientExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
		    	 clientExecutor.shutdownNow(); // Cancel currently executing tasks
		       // Wait a while for tasks to respond to being cancelled
		       if (!clientExecutor.awaitTermination(5, TimeUnit.SECONDS))
		           System.err.println("Pool did not terminate");
		     }
		   } catch (InterruptedException ie) {
		     // (Re-)Cancel if current thread also interrupted
			   clientExecutor.shutdownNow();
		     // Preserve interrupt status
		     Thread.currentThread().interrupt();
		   }

	}

	public boolean isRunning() {
		return this.running;
	}

	public int getPhase() {
		return this.phase;
	}

	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	
	
	
}
