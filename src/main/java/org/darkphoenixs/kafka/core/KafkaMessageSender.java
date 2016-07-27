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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>Title: KafkaMessageSender</p>
 * <p>Description: Kafka消息发送接口</p>
 *
 * @author Victor.Zxy
 * @version 1.0
 * @since 2015-06-01
 */
public interface KafkaMessageSender<K, V> {

    /**
     * logger
     */
    Logger logger = LoggerFactory.getLogger(KafkaMessageSender.class);

    /**
     * <p>Title: send</p>
     * <p>Description: Send the msg to Kafka</p>
     *
     * @param topic topic name
     * @param value data to be sent
     */
    void send(String topic, V value);

    /**
     * <p>Title: sendWithKey</p>
     * <p>Description: Send the msg to Kafka</p>
     *
     * @param topic topic name
     * @param key   the key of data
     * @param value data to be sent
     */
    void sendWithKey(String topic, K key, V value);

    /**
     * <p>Title: shutDown</p>
     * <p>Description: Shutdown this sender, so it could not be used again.</p>
     */
    void shutDown();
}
