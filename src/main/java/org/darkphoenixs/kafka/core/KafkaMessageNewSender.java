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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Properties;

/**
 * <p>Title: KafkaMessageNewSender</p>
 * <p>Description: Kafka消息新发送器</p>
 *
 * @param <K> the type of kafka message key
 * @param <V> the type of kafka message value
 * @author Victor.Zxy
 * @version 1.4.0
 * @see KafkaMessageSender
 * @since 2016 /7/25.
 */
public class KafkaMessageNewSender<K, V> implements KafkaMessageSender<K, V> {

    /**
     * The producer is thread safe and sharing a single producer instance
     * across threads will generally be faster than having multiple instances.
     */
    private final KafkaProducer<K, V> kafkaProducer;

    /**
     * @param properties the properties
     */
    public KafkaMessageNewSender(Properties properties) {

        kafkaProducer = new KafkaProducer<K, V>(properties);
    }

    /**
     * Gets topic partitions.
     *
     * @param topic the topic
     * @return the partitions
     */
    public List<PartitionInfo> getPartitions(String topic) {

        return kafkaProducer.partitionsFor(topic);
    }

    @Override
    public void send(String topic, V value) {

        kafkaProducer.send(new ProducerRecord<K, V>(topic, value), sendCallback);
    }

    @Override
    public void sendWithKey(String topic, K key, V value) {

        kafkaProducer.send(new ProducerRecord<K, V>(topic, key, value), sendCallback);
    }

    @Override
    public void shutDown() {

        kafkaProducer.flush();

        kafkaProducer.close();
    }

    /**
     * The Send callback.
     */
    protected Callback sendCallback = new Callback() {

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {

            if (exception != null)
                logger.error("Send message failed.", exception);
        }
    };
}
