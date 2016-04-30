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
package org.darkphoenixs.mq.codec;

import java.util.List;

import org.darkphoenixs.mq.exception.MQException;

/**
 * <p>Title: MessageEncoder</p>
 * <p>Description: 消息编码器接口</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @version 1.0
 */
public interface MessageEncoder<T> {

	/**
	 * <p>Title: encode</p>
	 * <p>Description: 消息序列化</p>
	 *
	 * @param message 消息
	 * @return 消息序列化
	 * @throws MQException
	 */
	byte[] encode(T message) throws MQException;
	
	/**
	 * <p>Title: batchEncode</p>
	 * <p>Description: 批量序列化</p>
	 *
	 * @param message 消息列表
	 * @return 消息序列化列表
	 * @throws MQException
	 */
	List<byte[]> batchEncode(List<T> message) throws MQException;
}
