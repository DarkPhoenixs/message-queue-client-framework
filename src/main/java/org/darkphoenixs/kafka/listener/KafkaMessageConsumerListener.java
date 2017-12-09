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

import org.darkphoenixs.kafka.consumer.AbstractConsumer;
import org.darkphoenixs.mq.exception.MQException;

import java.util.Map;

/**
 * <p>KafkaMessageConsumerListener</p>
 * <p>Kafka消费者监听器</p>
 *
 * @author Victor.Zxy
 * @version 1.3.0
 * @see KafkaMessageListener
 * @since 2016年7月21日
 */
public class KafkaMessageConsumerListener<K, V> extends KafkaMessageListener<K, V> {

    /**
     * abstractConsumer
     */
    private AbstractConsumer<K, V> consumer;

    /**
     * @return the consumer
     */
    public AbstractConsumer<K, V> getConsumer() {
        return consumer;
    }

    /**
     * @param consumer the consumer to set
     */
    public void setConsumer(AbstractConsumer<K, V> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onMessage(K key, V val) throws MQException {

        if (consumer != null)

            consumer.receive(key, val);
        else
            throw new MQException("MQConsumer is null !");
    }

    @Override
    public void onMessage(Map<K, V> messages) throws MQException {

        if (consumer != null)

            consumer.receive(messages);
        else
            throw new MQException("MQConsumer is null !");
    }
}
