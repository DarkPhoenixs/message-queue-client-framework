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
package org.darkphoenixs.kafka.core;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.darkphoenixs.kafka.pool.KafkaMessageSenderPool;

/**
 * <p>Title: KafkaMessageSenderImpl</p>
 * <p>Description: Kafka消息发送实现类</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see KafkaMessageSender
 * @version 1.0
 */
public class KafkaMessageSenderImpl<K, V> implements KafkaMessageSender<K, V> {

	/** producer */
	private Producer<K, V> producer;

	/** pool */
	private KafkaMessageSenderPool<K, V> pool;

	/**
	 * @return the producer
	 */
	public Producer<K, V> getProducer() {
		return producer;
	}

	/**
	 * @param producer
	 *            the producer to set
	 */
	public void setProducer(Producer<K, V> producer) {
		this.producer = producer;
	}

	/**
	 * @return the pool
	 */
	public KafkaMessageSenderPool<K, V> getPool() {
		return pool;
	}

	/**
	 * @param pool
	 *            the pool to set
	 */
	public void setPool(KafkaMessageSenderPool<K, V> pool) {
		this.pool = pool;
	}

	/**
	 * Construction method.
	 * 
	 * @param props
	 *            param props
	 * @param pool
	 *            sender Pool
	 */
	public KafkaMessageSenderImpl(Properties props,
			KafkaMessageSenderPool<K, V> pool) {
		super();
		ProducerConfig config = new ProducerConfig(props);
		this.producer = new Producer<K, V>(config);
		this.pool = pool;
	}

	@Override
	public void send(String topic, V value) {
		KeyedMessage<K, V> data = new KeyedMessage<K, V>(topic, value);
		this.producer.send(data);
	}

	@Override
	public void sendWithKey(String topic, K key, V value) {
		KeyedMessage<K, V> data = new KeyedMessage<K, V>(topic, key, value);
		this.producer.send(data);
	}

	@Override
	public void close() {
		this.pool.returnSender(this);
	}

	@Override
	public void shutDown() {
		this.producer.close();
	}

}
