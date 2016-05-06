/*
 * Copyright 2015-2016 Dark Phoenixs (Open-Source Organization).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.darkphoenixs.kafka.pool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.darkphoenixs.kafka.core.KafkaConstants;
import org.darkphoenixs.kafka.core.KafkaMessageSender;
import org.darkphoenixs.kafka.core.KafkaMessageSenderImpl;
import org.darkphoenixs.kafka.core.ZookeeperBrokers;
import org.darkphoenixs.kafka.core.ZookeeperHosts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

/**
 * <p>Title: KafkaMessageSenderPool</p>
 * <p>Description: Kafka消息发送连接池</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @version 1.0
 */
public class KafkaMessageSenderPool<K, V> {

	private static final String tagger = "KafkaMessageSenderPool";

	private static final Logger logger = LoggerFactory
			.getLogger(KafkaMessageSenderPool.class);

	private static final int defaultSize = Runtime.getRuntime()
			.availableProcessors() * 2 + 1;

	/** freeSender */
	protected Semaphore freeSender;
	/** queue */
	protected LinkedBlockingQueue<KafkaMessageSender<K, V>> queue;
	/** pool */
	protected ExecutorService pool;
	/** closingLock */
	protected ReadWriteLock closingLock = new ReentrantReadWriteLock();
	/** props */
	protected Properties props = new Properties();

	/** poolSize */
	private int poolSize;
	/** config */
	private Resource config;
	
	/** threadFactory */
	private ThreadFactory threadFactory;
	
	/**
	 * Init threadFactory.
	 */
	public KafkaMessageSenderPool() {

		this.threadFactory = new KafkaPoolThreadFactory(tagger, true);
	}
	
	/**
	 * @return the threadFactory
	 */
	public ThreadFactory getThreadFactory() {
		return threadFactory;
	}

	/**
	 * @param threadFactory the threadFactory to set
	 */
	public void setThreadFactory(ThreadFactory threadFactory) {
		this.threadFactory = threadFactory;
	}

	/**
	 * @param poolSize
	 *            the poolSize to set
	 */
	public void setPoolSize(int poolSize) {
		if (poolSize < defaultSize)
			poolSize = defaultSize;
		this.poolSize = poolSize;
		this.freeSender = new Semaphore(poolSize);
		this.queue = new LinkedBlockingQueue<KafkaMessageSender<K, V>>(poolSize);
		this.pool = Executors.newFixedThreadPool(poolSize, threadFactory);
	}
	
	/**
	 * @param zkhosts
	 *            the zkhosts to set
	 */
	public void setZkhosts(ZookeeperHosts zkhosts) {
		ZookeeperBrokers brokers = new ZookeeperBrokers(
				zkhosts.getBrokerZkStr(), zkhosts.getBrokerZkPath(),
				zkhosts.getTopic());
		this.setBrokerStr(brokers.getBrokerInfo());
		brokers.close();
	}

	/**
	 * @param config
	 *            the config to set
	 */
	public void setConfig(Resource config) {
		this.config = config;
		try {
			PropertiesLoaderUtils.fillProperties(props, this.config);
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * @param clientId the clientId to set
	 */
	public void setClientId(String clientId) {
		props.setProperty(KafkaConstants.CLIENT_ID, clientId);
	}
	
	/**
	 * @param brokerStr
	 *            the brokerStr to set
	 */
	public void setBrokerStr(String brokerStr) {
		props.setProperty(KafkaConstants.BROKER_LIST, brokerStr);
	}

	/**
	 * @return the clientId
	 */
	public String getClientId() {
		return props.getProperty(KafkaConstants.CLIENT_ID);
	}
	
	/**
	 * @return the brokerStr
	 */
	public String getBrokerStr() {
		return props.getProperty(KafkaConstants.BROKER_LIST);
	}

	/**
	 * @return the poolSize
	 */
	public int getPoolSize() {
		return poolSize;
	}

	/**
	 * @return the config
	 */
	public Resource getConfig() {
		return config;
	}

	/**
	 * @return the props
	 */
	public Properties getProps() {
		return props;
	}

	/**
	 * @param props the props to set
	 */
	public void setProps(Properties props) {
		this.props = props;
	}

	/**
	 * Init the pool.
	 */
	public synchronized void init() {
		
		if( poolSize ==0 )
			this.setPoolSize(defaultSize);
		
		logger.info("Message sender pool initializing. poolSize : " + poolSize + " config : " + props.toString());
		
		List<Callable<Boolean>> taskList = new ArrayList<Callable<Boolean>>();
		final CountDownLatch count = new CountDownLatch(poolSize);
		for (int i = 0; i < poolSize; i++) {
			taskList.add(new InitTask(count, this));
		}

		try {
			pool.invokeAll(taskList);
			count.await(KafkaConstants.INIT_TIMEOUT_MIN, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			logger.error("Failed to init the MessageSenderPool", e);
		}
	}

	/**
	 * Get a sender from the pool within the given timeout
	 * 
	 * @param waitTimeMillis
	 *            how long should it wait for getting the sender instance
	 * @return a sender instance
	 */
	public KafkaMessageSender<K, V> getSender(long waitTimeMillis) {
		try {
			if (!freeSender.tryAcquire(waitTimeMillis, TimeUnit.MILLISECONDS))
				throw new RuntimeException(
						"Timeout waiting for idle object in the pool.");

		} catch (InterruptedException e) {
			throw new RuntimeException(
					"Interrupted waiting for idle object in the pool .");
		}
		KafkaMessageSender<K, V> sender = null;

		closingLock.readLock().lock();
		try {
			sender = queue.poll();
			if (sender == null) {
				sender = new KafkaMessageSenderImpl<K, V>(props, this);
				if (sender != null) {
					logger.info("Add new sender to the pool.");
					queue.offer(sender);
				}
			}
		} catch (Exception e) {
			logger.error("Failed to get the MessageSender", e);
		} finally {
			closingLock.readLock().unlock();
		}

		return sender;
	}

	/**
	 * Return a sender back to pool.
	 */
	public void returnSender(KafkaMessageSender<K, V> sender) {
		if (this.queue.contains(sender))
			return;
		this.queue.offer(sender);
		this.freeSender.release();
	}

	/**
	 * Close the pool.
	 */
	public synchronized void destroy() {
		
		logger.info("Message sender pool closing.");
		
		// lock the thread for closing
		closingLock.writeLock().lock();
		try {
			List<Callable<Boolean>> taskList = new ArrayList<Callable<Boolean>>();
			int size = queue.size();
			final CountDownLatch count = new CountDownLatch(size);
			for (int i = 0; i < size; i++) {
				taskList.add(new DestroyTask(count));
			}

			pool.invokeAll(taskList);
			count.await(KafkaConstants.INIT_TIMEOUT_MIN, TimeUnit.MINUTES);
			pool.shutdownNow();

		} catch (Exception e) {
			logger.error("Failed to close the MessageSenderPool", e);
		} finally {
			closingLock.writeLock().unlock();
		}
	}

	/**
	 * Init Task Call Back.
	 */
	class InitTask implements Callable<Boolean> {
		
		CountDownLatch count;
		KafkaMessageSenderPool<K, V> pool;

		public InitTask(CountDownLatch count, KafkaMessageSenderPool<K, V> pool) {
			this.count = count;
			this.pool = pool;
		}

		@Override
		public Boolean call() throws Exception {
			KafkaMessageSender<K, V> sender = new KafkaMessageSenderImpl<K, V>(
					props, pool);
			if (sender != null) {
				queue.offer(sender);
				count.countDown();
			}
			return true;
		}
	}
	
	/**
	 * Destroy Task Call Back.
	 */
	class DestroyTask implements Callable<Boolean> {
		
		CountDownLatch count;

		public DestroyTask(CountDownLatch count) {
			this.count = count;
		}
		
		@Override
		public Boolean call() throws Exception {
			KafkaMessageSender<K, V> sender = queue.poll();
			if (sender != null) {
				sender.shutDown();
				count.countDown();
			}
			return true;
		}
		
	}
}
