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

import org.darkphoenixs.mq.exception.MQException;

/**
 * <p>Title: MessageProducer</p>
 * <p>Description: 消息生产者</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see AbstractProducer
 * @version 1.0
 */
public class MessageProducer<T> extends AbstractProducer<T> {

	@Override
	protected T doSend(T message) throws MQException {

		return message;
	}
}
