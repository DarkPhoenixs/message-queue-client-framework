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

import java.util.List;
import java.util.Map;

import org.darkphoenixs.kafka.codec.KafkaMessageDecoder;
import org.darkphoenixs.kafka.codec.KafkaMessageEncoder;
import org.darkphoenixs.kafka.pool.KafkaMessageReceiverPool;
import org.darkphoenixs.kafka.pool.KafkaMessageSenderPool;
import org.darkphoenixs.kafka.pool.MessageReceiverPool;
import org.darkphoenixs.kafka.pool.MessageSenderPool;
import org.darkphoenixs.mq.exception.MQException;

/**
 * <p>Title: KafkaMessageTemplate</p>
 * <p>Description: Kafka消息模板类</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @version 1.0
 */
public class KafkaMessageTemplate<K, V> {

	/** messageSenderPool */
	private MessageSenderPool<byte[], byte[]> messageSenderPool;
	
	/** messageReceiverPool */
	private MessageReceiverPool<byte[], byte[]> messageReceiverPool;
	
	/** encoder */
	private KafkaMessageEncoder<K, V> encoder;
	
	/** decoder */
	private KafkaMessageDecoder<K, V> decoder;
	
	/**
	 * @param messageSenderPool the messageSenderPool to set
	 */
	public void setMessageSenderPool(
			KafkaMessageSenderPool<byte[], byte[]> messageSenderPool) {
		this.messageSenderPool = messageSenderPool;
	}
	
	/**
	 * @return the messageSenderPool
	 */
	public MessageSenderPool<byte[], byte[]> getMessageSenderPool() {
		return messageSenderPool;
	}
	
	/**
	 * @return the messageReceiverPool
	 */
	public MessageReceiverPool<byte[], byte[]> getMessageReceiverPool() {
		return messageReceiverPool;
	}

	/**
	 * @param messageReceiverPool the messageReceiverPool to set
	 */
	public void setMessageReceiverPool(
			KafkaMessageReceiverPool<byte[], byte[]> messageReceiverPool) {
		this.messageReceiverPool = messageReceiverPool;
	}

	/**
	 * @param encoder the encoder to set
	 */
	public void setEncoder(KafkaMessageEncoder<K, V> encoder) {
		this.encoder = encoder;
	}
	
	/**
	 * @return the encoder
	 */
	public KafkaMessageEncoder<K, V> getEncoder() {
		return encoder;
	}
	
	/**
	 * @return the decoder
	 */
	public KafkaMessageDecoder<K, V> getDecoder() {
		return decoder;
	}

	/**
	 * @param decoder the decoder to set
	 */
	public void setDecoder(KafkaMessageDecoder<K, V> decoder) {
		this.decoder = decoder;
	}

	/**
	 * <p>Title: send</p>
	 * <p>Description: 发送消息</p>
	 *
	 * @param destination 队列
	 * @param message 消息
	 */
	public void send(KafkaDestination destination, byte[] message) throws MQException {
		
		KafkaMessageSender<byte[], byte[]> sender = messageSenderPool.getSender();

		sender.send(destination.getDestinationName(), message);

		sender.close();
	}
	
	/**
	 * <p>sendWithKey</p>
	 * <p>发送消息带标识</p>
	 *
	 * @param destination 队列
	 * @param key 标识
	 * @param message 消息
	 * @since 1.3.0
	 */
	public void sendWithKey(KafkaDestination destination, byte[] key, byte[] message) {
		
		KafkaMessageSender<byte[], byte[]> sender = messageSenderPool.getSender();

		sender.sendWithKey(destination.getDestinationName(), key, message);

		sender.close();
	}
	
	/**
	 * <p>Title: convertAndSend</p>
	 * <p>Description: 转换并发送消息</p>
	 *
	 * @param destination 队列
	 * @param message 消息
	 * @throws MQException 
	 */
	public void convertAndSend(KafkaDestination destination, V message) throws MQException {
		
		byte[] encoded = encoder.encode(message);
		
		this.send(destination, encoded);
	}
	
	/**
	 * <p>convertAndSendWithKey</p>
	 * <p>转换并发送消息带标识</p>
	 *
	 * @param destination 队列
	 * @param key 标识
	 * @param message 消息
	 * @throws MQException
	 * @since 1.3.0
	 */
	public void convertAndSendWithKey(KafkaDestination destination, K key, V message)  throws MQException {
		
		byte[] encodeKey = encoder.encodeKey(key);
		
		byte[] encodeVal = encoder.encodeVal(message);
		
		this.sendWithKey(destination, encodeKey, encodeVal);
	}
	
	/**
	 * <p>Title: receive</p>
	 * <p>Description: 接收消息</p>
	 *
	 * @param destination 队列
	 * @param partition 分区编号
	 * @param beginOffset 起始位置
	 * @param readOffset 读取条数
	 * @return 消息列表
	 * @throws MQException
	 */
	public List<byte[]> receive(KafkaDestination destination, int partition, long beginOffset, long readOffset) throws MQException {
		
		KafkaMessageReceiver<byte[], byte[]> receiver = messageReceiverPool.getReceiver();
		
		List<byte[]> messages = receiver.receive(destination.getDestinationName(), partition, beginOffset, readOffset);
		
		messageReceiverPool.returnReceiver(receiver);
		
		return messages;
	}
	
	/**
	 * <p>receiveWithKey</p>
	 * <p>接收消息带标识</p>
	 *
	 * @param destination 队列
	 * @param partition 分区编号
	 * @param beginOffset 起始位置
	 * @param readOffset 读取条数
	 * @return 消息列表
	 * @throws MQException
	 * @since 1.3.0
	 */
	public Map<byte[], byte[]> receiveWithKey(KafkaDestination destination, int partition, long beginOffset, long readOffset) throws MQException {
		
		KafkaMessageReceiver<byte[], byte[]> receiver = messageReceiverPool.getReceiver();

		Map<byte[], byte[]> messages = receiver.receiveWithKey(destination.getDestinationName(), partition, beginOffset, readOffset);
		
		messageReceiverPool.returnReceiver(receiver);
		
		return messages;
	}
	
	/**
	 * <p>Title: receiveAndConvert</p>
	 * <p>Description: 接收并转换消息</p>
	 *
	 * @param destination 队列
	 * @param partition 分区编号
	 * @param beginOffset 起始位置
	 * @param readOffset 读取条数
	 * @return 消息列表
	 * @throws MQException
	 */
	public List<V> receiveAndConvert(KafkaDestination destination, int partition, long beginOffset, long readOffset) throws MQException {
		
		List<byte[]> decoded = this.receive(destination, partition, beginOffset, readOffset);
		
		return decoder.batchDecode(decoded);
	}
	
	/**
	 * <p>receiveWithKeyAndConvert</p>
	 * <p>接收带标识并转换消息</p>
	 *
	 * @param destination 队列
	 * @param partition 分区编号
	 * @param beginOffset 起始位置
	 * @param readOffset 读取条数
	 * @return 消息列表
	 * @throws MQException
	 * @since 1.3.0
	 */
	public Map<K, V> receiveWithKeyAndConvert(KafkaDestination destination, int partition, long beginOffset, long readOffset) throws MQException {
		
		Map<byte[], byte[]> decoded = this.receiveWithKey(destination, partition, beginOffset, readOffset);
		
		return decoder.batchDecode(decoded);
	}

}
