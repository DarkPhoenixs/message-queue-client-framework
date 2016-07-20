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
package org.darkphoenixs.mq.common;

import java.util.concurrent.ConcurrentHashMap;

import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.factory.ProducerFactory;
import org.darkphoenixs.mq.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: MessageProducerFactory</p>
 * <p>Description: 消息生产者工厂</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see ProducerFactory
 * @version 1.0
 */
public class MessageProducerFactory implements ProducerFactory {

	/** logger */
	protected Logger logger = LoggerFactory.getLogger(MessageProducerFactory.class);

	/** instance */
	private static MessageProducerFactory instance;

	/** producers */
	private Producer<?>[] producers;

	/** producerCache */
	private ConcurrentHashMap<String, Producer<?>> producerCache = new ConcurrentHashMap<String, Producer<?>>();

	/**
	 * @param producers
	 *            the producers to set
	 */
	public void setProducers(Producer<?>[] producers) {
		this.producers = producers;
	}

	/**
	 * private construction method
	 */
	private MessageProducerFactory() {
	}

	/**
	 * get singleton instance method
	 */
	public synchronized static ProducerFactory getInstance() {

		if (instance == null)
			instance = new MessageProducerFactory();
		return instance;
	}

	@Override
	public <T> void addProducer(Producer<T> producer) throws MQException {

		producerCache.put(producer.getProducerKey(), producer);

		logger.debug("Add Producer : " + producer.getProducerKey());

	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Producer<T> getProducer(String producerKey) throws MQException {

		if (producerCache.containsKey(producerKey)) {

			logger.debug("Get Producer : " + producerKey);

			return (Producer<T>) producerCache.get(producerKey);

		} else {

			logger.warn("Unknown ProducerKey : " + producerKey);

			return null;
		}
	}

	@Override
	public void init() throws MQException {

		if (producers != null)

			for (int i = 0; i < producers.length; i++)

				producerCache.put(producers[i].getProducerKey(), producers[i]);

	}

	@Override
	public void destroy() throws MQException {

		if (producers != null)
			producers = null;

		if (instance != null)
			instance = null;

		producerCache.clear();

		logger.debug("Destroyed!");
	}
}
