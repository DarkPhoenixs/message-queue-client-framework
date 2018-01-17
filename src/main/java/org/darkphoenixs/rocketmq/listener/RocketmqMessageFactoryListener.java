/*
 * Copyright (c) 2017. Dark Phoenixs (Open-Source Organization).
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

package org.darkphoenixs.rocketmq.listener;

import org.darkphoenixs.mq.consumer.MQConsumer;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.factory.MQConsumerFactory;
import org.darkphoenixs.mq.util.RefleTool;
import org.darkphoenixs.rocketmq.consumer.AbstractConsumer;

import java.util.List;
import java.util.Map;

/**
 * <p>Title: RocketmqMessageFactoryListener</p>
 * <p>Description: Rocketmq消息监听器工厂类</p>
 *
 * @param <T> the type parameter
 * @author Victor
 * @version 1.0
 * @see RocketmqMessageConsumerListener
 * @since 2017 /12/10
 */
public class RocketmqMessageFactoryListener<T> extends RocketmqMessageConsumerListener<T> {

    /**
     * consumerKeyField
     */
    private String consumerKeyField;

    /**
     * consumerFactory
     */
    private MQConsumerFactory consumerFactory;

    /**
     * Gets consumer key field.
     *
     * @return the consumer key field
     */
    public String getConsumerKeyField() {
        return consumerKeyField;
    }

    /**
     * Sets consumer key field.
     *
     * @param consumerKeyField the consumer key field
     */
    public void setConsumerKeyField(String consumerKeyField) {
        this.consumerKeyField = consumerKeyField;
    }

    /**
     * Gets consumer factory.
     *
     * @return the consumer factory
     */
    public MQConsumerFactory getConsumerFactory() {
        return consumerFactory;
    }

    /**
     * Sets consumer factory.
     *
     * @param consumerFactory the consumer factory
     */
    public void setConsumerFactory(MQConsumerFactory consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    @Deprecated
    public void onMessage(T message) throws MQException {

        if (message == null)
            throw new MQException("Message is null !");

        if (consumerFactory == null)
            throw new MQException("ConsumerFactory is null !");

        if (consumerKeyField == null)
            throw new MQException("ConsumerKeyField is null !");

        String consumerKey = RefleTool.getMethodValue(message, "get" + consumerKeyField.substring(0, 1).toUpperCase() + consumerKeyField.substring(1));

        if (consumerKey == null)
            throw new MQException("Consumer Key is null !");

        MQConsumer<T> consumer = consumerFactory.getConsumer(consumerKey);

        if (consumer == null)
            throw new MQException("Consumer is null !");

        consumer.receive(message);
    }

    @Deprecated
    public void onMessage(List<T> messages) throws MQException {

        throw new MQException("MessageFactoryListener is not support onMessage(List messages) !");
    }

    @Override
    public void onMessage(String key, T message) throws MQException {

        if (message == null)
            throw new MQException("Message is null !");

        if (consumerFactory == null)
            throw new MQException("ConsumerFactory is null !");

        if (consumerKeyField == null)
            throw new MQException("ConsumerKeyField is null !");

        String consumerKey = RefleTool.getMethodValue(message, "get" + consumerKeyField.substring(0, 1).toUpperCase() + consumerKeyField.substring(1));

        if (consumerKey == null)
            throw new MQException("Consumer Key is null !");

        AbstractConsumer<T> consumer = (AbstractConsumer<T>) consumerFactory.getConsumer(consumerKey);

        if (consumer == null)
            throw new MQException("Consumer is null !");

        consumer.receive(key, message);
    }

    @Override
    public void onMessage(Map<String, T> messages) throws MQException {

        throw new MQException("MessageFactoryListener is not support onMessage(Map messages) !");
    }
}
