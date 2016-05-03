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
package org.darkphoenixs.activemq.producer;

import javax.jms.Destination;
import javax.jms.Queue;
import javax.jms.Topic;

import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;

/**
 * <p>Title: AbstractProducer</p>
 * <p>Description: 生产者抽象类</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see Producer
 * @version 1.0
 */
public abstract class AbstractProducer<T> implements Producer<T> {

	/** logger */
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	/** jmsTemplate */
	private JmsTemplate jmsTemplate;

	/** destination */
	private Destination destination;

	/**
	 * @return the jmsTemplate
	 */
	public JmsTemplate getJmsTemplate() {
		return jmsTemplate;
	}

	/**
	 * @param jmsTemplate
	 *            the jmsTemplate to set
	 */
	public void setJmsTemplate(JmsTemplate jmsTemplate) {
		this.jmsTemplate = jmsTemplate;
	}

	/**
	 * @return the destination
	 */
	public Destination getDestination() {
		return destination;
	}

	/**
	 * @param destination
	 *            the destination to set
	 */
	public void setDestination(Destination destination) {
		this.destination = destination;
	}

	@Override
	public void send(T message) throws MQException {

		try {
			Object obj = doSend(message);

			jmsTemplate.convertAndSend(destination, obj);

		} catch (Exception e) {

			throw new MQException(e);
		}

		logger.debug("Send Success, ProducerKey : " + this.getProducerKey()
				+ " , Message : " + message);

	}

	@Override
	public String getProducerKey() throws MQException {

		if (destination instanceof Queue)

			try {
				return ((Queue) destination).getQueueName();

			} catch (Exception e) {

				throw new MQException(e);
			}

		else if (destination instanceof Topic)

			try {
				return ((Topic) destination).getTopicName();

			} catch (Exception e) {

				throw new MQException(e);
			}

		else
			return destination.toString();
	}

	/**
	 * <p>Title: doSend</p>
	 * <p>Description: 消息发送方法</p>
	 *
	 * @param message 消息
	 * @return 消息
	 * @throws MQException MQ异常
	 */
	protected abstract Object doSend(T message) throws MQException;
}
