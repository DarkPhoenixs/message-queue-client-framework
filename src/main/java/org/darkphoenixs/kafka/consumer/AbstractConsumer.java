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
package org.darkphoenixs.kafka.consumer;

import org.darkphoenixs.mq.consumer.MQConsumer;
import org.darkphoenixs.mq.exception.MQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * <p>AbstractConsumer</p>
 * <p>消费者抽象类</p>
 *
 * @param <K> the type parameter
 * @param <V> the type parameter
 * @author Victor.Zxy
 * @version 1.3.0
 * @see MQConsumer
 * @since 2016年7月21日
 */
public abstract class AbstractConsumer<K, V> implements MQConsumer<V> {

    /**
     * logger
     */
    protected Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * consumerKey
     */
    private String consumerKey;

    @Override
    public String getConsumerKey() throws MQException {
        return this.consumerKey;
    }

    /**
     * Sets consumer key.
     *
     * @param consumerKey the consumerKey to set
     */
    public void setConsumerKey(String consumerKey) {
        this.consumerKey = consumerKey;
    }

    /**
     * <p>receive</p>
     * <p>接收消息</p>
     *
     * @param key 标识
     * @param Val 消息
     * @throws MQException the mq exception
     */
    public void receive(K key, V Val) throws MQException {

        try {
            doReceive(key, Val);

        } catch (Exception e) {

            throw new MQException(e);
        }

        logger.debug("Receive Success, ConsumerKey : " + this.getConsumerKey()
                + " , MessageKey : " + key + " , Message : " + Val);
    }

    /**
     * <p>receive</p>
     * <p>接收消息</p>
     *
     * @param messages 消息键值对
     * @throws MQException the mq exception
     * @since 1.4.3
     */
    public void receive(Map<K, V> messages) throws MQException {

        try {
            doReceive(messages);

        } catch (Exception e) {

            throw new MQException(e);
        }

        logger.debug("Receive Success, Batch size : " + messages.size());
    }

    @Override
    public void receive(V message) throws MQException {

        try {
            doReceive(message);

        } catch (Exception e) {

            throw new MQException(e);
        }

        logger.debug("Receive Success, ConsumerKey : " + this.getConsumerKey()
                + " , Message : " + message);
    }

    /**
     * <p>doReceive</p>
     * <p>消息接收方法（兼容无Key）</p>
     *
     * @param message 消息
     * @throws MQException the mq exception
     * @since 1.4.0
     */
    protected void doReceive(V message) throws MQException {
        // For compatible without Key.
    }

    /**
     * <p>doReceive</p>
     * <p>消息接收方法（批量消费）</p>
     *
     * @param messages 消息键值对
     * @throws MQException the mq exception
     * @since 1.4.3
     */
    protected void doReceive(Map<K, V> messages) throws MQException {
        // Batch consumer messages override this.
    }

    /**
     * <p>doReceive</p>
     * <p>消息接收方法</p>
     *
     * @param key 标识
     * @param val 消息
     * @throws MQException MQ异常
     */
    protected abstract void doReceive(K key, V val) throws MQException;
}
