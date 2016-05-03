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
package org.darkphoenixs.kafka.producer;

import org.darkphoenixs.kafka.core.KafkaDestination;
import org.darkphoenixs.kafka.core.KafkaMessageTemplate;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: AbstractProducer</p>
 * <p>Description: 生产者抽象类</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see Producer
 * @version 1.0
 */
public abstract class AbstractProducer<T> implements Producer<T>  {

	/** logger */
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	/** messageTemplate */
	private KafkaMessageTemplate<T> messageTemplate;
	
	/** destination */
	private KafkaDestination destination;
	
	/**
	 * @return the messageTemplate
	 */
	public KafkaMessageTemplate<T> getMessageTemplate() {
		return messageTemplate;
	}

	/**
	 * @param messageTemplate the messageTemplate to set
	 */
	public void setMessageTemplate(KafkaMessageTemplate<T> messageTemplate) {
		this.messageTemplate = messageTemplate;
	}
	
	/**
	 * @return the destination
	 */
	public KafkaDestination getDestination() {
		return destination;
	}

	/**
	 * @param destination the destination to set
	 */
	public void setDestination(KafkaDestination destination) {
		this.destination = destination;
	}

	@Override
	public void send(T message) throws MQException {

		try {
			T obj = doSend(message);

			messageTemplate.convertAndSend(destination, obj);

		} catch (Exception e) {

			throw new MQException(e);
		}

		logger.debug("Send Success, ProducerKey : " + this.getProducerKey()
				+ " , Message : " + message);

	}
	
	@Override
	public String getProducerKey() throws MQException {

		return destination.getDestinationName();
	}
	
	/**
	 * <p>Title: doSend</p>
	 * <p>Description: 消息发送方法</p>
	 *
	 * @param message 消息
	 * @return 消息
	 * @throws MQException MQ异常
	 */
	protected abstract T doSend(T message) throws MQException;
}
