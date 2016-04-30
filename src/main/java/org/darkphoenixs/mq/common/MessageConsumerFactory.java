/*
 * Copyright 2014-2024 Dark Phoenixs (Open-Source Organization).
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
package org.darkphoenixs.mq.common;

import java.util.concurrent.ConcurrentHashMap;

import org.darkphoenixs.mq.consumer.Consumer;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.factory.ConsumerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	/** logger */
	protected Logger logger = LoggerFactory.getLogger(MessageConsumerFactory.class);

	/** instance */
	private static MessageConsumerFactory instance;

	/** consumers */
	private Consumer<?>[] consumers;

	/** consumerCache */
	private ConcurrentHashMap<String, Consumer<?>> consumerCache = new ConcurrentHashMap<String, Consumer<?>>();

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
