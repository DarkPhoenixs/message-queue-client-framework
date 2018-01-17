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
package org.darkphoenixs.kafka.listener;

import org.darkphoenixs.mq.consumer.MQConsumer;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.factory.MQConsumerFactory;
import org.darkphoenixs.mq.util.RefleTool;

/**
 * <p>Title: MessageFactoryConsumerListener</p>
 * <p>Description: 消费者工厂监听器</p>
 *
 * @author Victor.Zxy
 * @version 1.0
 * @see KafkaMessageListener
 * @since 2015-06-01
 */
@Deprecated
public class MessageFactoryConsumerListener<K, V> extends KafkaMessageListener<K, V> {

    /**
     * consumerKeyField
     */
    private String consumerKeyField;

    /**
     * consumerFactory
     */
    private MQConsumerFactory consumerFactory;

    /**
     * @return the consumerKeyField
     */
    public String getConsumerKeyField() {
        return consumerKeyField;
    }

    /**
     * @param consumerKeyField the consumerKeyField to set
     */
    public void setConsumerKeyField(String consumerKeyField) {
        this.consumerKeyField = consumerKeyField;
    }

    /**
     * @return the consumerFactory
     */
    public MQConsumerFactory getConsumerFactory() {
        return consumerFactory;
    }

    /**
     * @param consumerFactory the consumerFactory to set
     */
    public void setConsumerFactory(MQConsumerFactory consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    @Override
    public void onMessage(V message) throws MQException {

        if (consumerFactory == null)
            throw new MQException("MQConsumerFactory is null !");

        if (consumerKeyField == null)
            throw new MQException("ConsumerKeyField is null !");

        if (message == null)
            throw new MQException("Message is null !");

        String consumerKey = RefleTool.getMethodValue(message, "get" + consumerKeyField.substring(0, 1).toUpperCase() + consumerKeyField.substring(1));

        if (consumerKey == null)
            throw new MQException("MQConsumer Key is null !");

        MQConsumer<V> consumer = consumerFactory.getConsumer(consumerKey);

        if (consumer == null)
            throw new MQException("MQConsumer is null !");

        consumer.receive(message);
    }
}
