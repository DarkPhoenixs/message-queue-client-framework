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
package org.darkphoenixs.kafka.core;

import org.darkphoenixs.mq.codec.MessageDecoder;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.listener.MessageListener;

/**
 * <p>Title: KafkaMessageAdapter</p>
 * <p>Description: Kafka消息适配器</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @version 1.0
 */
public class KafkaMessageAdapter<T> {

	/** decoder */
	private MessageDecoder<T> decoder;
	
	/** consumerListener */
	private MessageListener<T> messageListener;

	/** destination */
	private KafkaDestination destination;
	
	/**
	 * @return the decoder
	 */
	public MessageDecoder<T> getDecoder() {
		return decoder;
	}

	/**
	 * @param decoder the decoder to set
	 */
	public void setDecoder(MessageDecoder<T> decoder) {
		this.decoder = decoder;
	}

	/**
	 * @return the messageListener
	 */
	public MessageListener<T> getMessageListener() {
		return messageListener;
	}

	/**
	 * @param messageListener the messageListener to set
	 */
	public void setMessageListener(MessageListener<T> messageListener) {
		this.messageListener = messageListener;
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

	/**
	 * <p>Title: messageAdapter</p>
	 * <p>Description: 消息适配方法</p>
	 *
	 * @param key 消息key
	 * @param value 消息 value
	 * @throws MQException
	 */
	public void messageAdapter(Object key, Object value) throws MQException{
		
		byte[] bytes = (byte[])value;
		
		T message = decoder.decode(bytes);
		
		messageListener.onMessage(message);
	}
}

