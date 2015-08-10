/**
 * <p>Title: MessageConsumerFactory.java</p>
 * <p>Description: MessageConsumerFactory</p>
 * <p>Package: org.darkphoenixs.mq.common</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.mq.common;

import java.util.concurrent.ConcurrentHashMap;

import org.darkphoenixs.mq.consumer.Consumer;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.factory.ConsumerFactory;

/**
 * <p>Title: MessageConsumerFactory</p>
 * <p>Description: 消息消费者工厂</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see ConsumerFactory
 * @version 1.0
 */
public class MessageConsumerFactory implements ConsumerFactory {

	/** instance */
	private static MessageConsumerFactory instance;

	/** consumers */
	private Consumer<?>[] consumers;

	/** consumerCache */
	private ConcurrentHashMap<String, Consumer<?>> consumerCache = new ConcurrentHashMap<>();

	/**
	 * @return the consumers
	 */
	public Consumer<?>[] getConsumers() {
		return consumers;
	}

	/**
	 * @param consumers
	 *            the consumers to set
	 */
	public void setConsumers(Consumer<?>[] consumers) {
		this.consumers = consumers;
	}

	/**
	 * private construction method
	 */
	private MessageConsumerFactory() {
	}

	/**
	 * get singleton instance method
	 */
	public synchronized static ConsumerFactory getInstance() {

		if (instance == null)
			instance = new MessageConsumerFactory();
		return instance;
	}

	@Override
	public <T> void addConsumer(Consumer<T> consumer) throws MQException {

		consumerCache.put(consumer.getConsumerKey(), consumer);

		logger.debug("Add Consumer : " + consumer.getConsumerKey());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Consumer<T> getConsumer(String consumerKey) throws MQException {
		
		if (consumerCache.containsKey(consumerKey)) {
			
			logger.debug("Get Consumer : " + consumerKey);
			
			return (Consumer<T>) consumerCache.get(consumerKey);
			
		} else {
			
			logger.warn("Unknown ConsumerKey : " + consumerKey);

			return null;
		}
	}

	@Override
	public void init() throws MQException {

		if (consumers != null)

			for (int i = 0; i < consumers.length; i++)

				consumerCache.put(consumers[i].getConsumerKey(), consumers[i]);

		logger.debug("Initialized!");

	}

	@Override
	public void destroy() throws MQException {

		if (consumers != null)
			consumers = null;

		if (instance != null)
			instance = null;

		if (consumerCache != null)
			consumerCache.clear();

		logger.debug("Destroyed!");
	}

}
