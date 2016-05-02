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
package org.darkphoenixs.kafka.listener;

import org.darkphoenixs.mq.consumer.Consumer;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.factory.ConsumerFactory;
import org.darkphoenixs.mq.listener.MessageListener;
import org.darkphoenixs.mq.util.RefleTool;

/**
 * <p>Title: MessageFactoryConsumerListener</p>
 * <p>Description: 消费者工厂监听器</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see MessageListener
 * @version 1.0
 */
public class MessageFactoryConsumerListener<T> implements MessageListener<T> {

	/** consumerKeyField */
	private String consumerKeyField;
	
	/** consumerFactory */
	private ConsumerFactory consumerFactory;

	/**
	 * @return the consumerKeyField
	 */
	public String getConsumerKeyField() {
		return consumerKeyField;
	}

	/**
	 * @param consumerKeyField
	 *            the consumerKeyField to set
	 */
	public void setConsumerKeyField(String consumerKeyField) {
		this.consumerKeyField = consumerKeyField;
	}
	
	/**
	 * @return the consumerFactory
	 */
	public ConsumerFactory getConsumerFactory() {
		return consumerFactory;
	}

	/**
	 * @param consumerFactory
	 *            the consumerFactory to set
	 */
	public void setConsumerFactory(ConsumerFactory consumerFactory) {
		this.consumerFactory = consumerFactory;
	}

	@Override
	public void onMessage(T message) throws MQException {

		if (consumerFactory == null)	
			throw new MQException("ConsumerFactory is null !");

		if (consumerKeyField == null)	
			throw new MQException("ConsumerKeyField is null !");
		
		if (message == null)	
			throw new MQException("Message is null !");
		
		String consumerKey = RefleTool.getFieldValue(message, consumerKeyField, String.class);
		
		if (consumerKey == null)	
			throw new MQException("Consumer Key is null !");
		
		Consumer<T> consumer = consumerFactory.getConsumer(consumerKey);
		
		if (consumer == null)
			throw new MQException("Consumer is null !");
		
		consumer.receive(message);
	}
}
