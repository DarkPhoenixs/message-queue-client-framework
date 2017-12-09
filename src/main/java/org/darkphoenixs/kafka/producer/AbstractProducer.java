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
package org.darkphoenixs.kafka.producer;

import org.darkphoenixs.kafka.core.KafkaDestination;
import org.darkphoenixs.kafka.core.KafkaMessageTemplate;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.producer.MQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: AbstractProducer</p>
 * <p>Description: 生产者抽象类</p>
 *
 * @author Victor.Zxy
 * @version 1.0
 * @see MQProducer
 * @since 2015-06-01
 */
public abstract class AbstractProducer<K, V> implements MQProducer<V> {

    /**
     * logger
     */
    protected Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * messageTemplate
     */
    private KafkaMessageTemplate<K, V> messageTemplate;

    /**
     * destination
     */
    private KafkaDestination destination;

    /**
     * @since 1.2.3 producerKey
     */
    private String producerKey;

    /**
     * @return the messageTemplate
     */
    public KafkaMessageTemplate<K, V> getMessageTemplate() {
        return messageTemplate;
    }

    /**
     * @param messageTemplate the messageTemplate to set
     */
    public void setMessageTemplate(KafkaMessageTemplate<K, V> messageTemplate) {
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
    public void send(V message) throws MQException {

        try {
            V obj = doSend(message);

            messageTemplate.convertAndSend(destination, obj);

        } catch (Exception e) {

            throw new MQException(e);
        }

        logger.debug("Send Success, ProducerKey : " + this.getProducerKey()
                + " , Message : " + message);
    }

    /**
     * <p>sendWithKey</p>
     * <p>发送消息带标识</p>
     *
     * @param key     标识
     * @param message 消息
     * @throws MQException
     * @since 1.3.0
     */
    public void sendWithKey(K key, V message) throws MQException {

        try {
            V obj = doSend(message);

            messageTemplate.convertAndSendWithKey(destination, key, obj);

        } catch (Exception e) {

            throw new MQException(e);
        }

        logger.debug("Send Success, ProducerKey : " + this.getProducerKey()
                + " , MessageKey : " + key + " , Message : " + message);
    }

    @Override
    public String getProducerKey() throws MQException {

        if (this.producerKey != null)

            return this.producerKey;

        return destination.getDestinationName();
    }

    /**
     * @param producerKey the producerKey to set
     * @since 1.2.3
     */
    public void setProducerKey(String producerKey) {
        this.producerKey = producerKey;
    }

    /**
     * <p>Title: doSend</p>
     * <p>Description: 消息发送方法</p>
     *
     * @param message 消息
     * @return 消息
     * @throws MQException MQ异常
     */
    protected abstract V doSend(V message) throws MQException;

}
