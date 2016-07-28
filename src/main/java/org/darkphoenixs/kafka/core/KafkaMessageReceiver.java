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

import java.util.List;
import java.util.Map;

/**
 * <p>Title: KafkaMessageReceiver</p>
 * <p>Description: Kafka消息接收接口</p>
 *
 * @param <K> the type parameter
 * @param <V> the type parameter
 * @author Victor.Zxy
 * @version 1.0
 * @since 2015 -06-01
 */
public interface KafkaMessageReceiver<K, V> {

    /**
     * logger
     */
    Logger logger = LoggerFactory.getLogger(KafkaMessageReceiver.class);

    /**
     * <p>Title: receive</p>
     * <p>Description: Receive the msg from Kafka</p>
     *
     * @param topic       Topic name
     * @param partition   Partition number
     * @param beginOffset Begin the offset index
     * @param readOffset  Number of read messages
     * @return message list
     */
    List<V> receive(String topic, int partition, long beginOffset, long readOffset);

    /**
     * <p>Title: receiveWithKey</p>
     * <p>Description: Receive the msg from Kafka</p>
     *
     * @param topic       Topic name
     * @param partition   Partition number
     * @param beginOffset Begin the offset index
     * @param readOffset  Number of read messages
     * @return message map
     */
    Map<K, V> receiveWithKey(String topic, int partition, long beginOffset, long readOffset);

    /**
     * <p>Title: getLatestOffset</p>
     * <p>Description: Get latest offset number</p>
     *
     * @param topic     Topic name
     * @param partition Partition number
     * @return the latest offset
     */
    long getLatestOffset(String topic, int partition);

    /**
     * <p>Title: getEarliestOffset</p>
     * <p>Description: Get earliest offset number</p>
     *
     * @param topic     Topic name
     * @param partition Partition number
     * @return the earliest offset
     */
    long getEarliestOffset(String topic, int partition);

    /**
     * <p>Title: getPartitionCount</p>
     * <p>Description: Get the partition count</p>
     *
     * @param topic the topic
     * @return the partitions
     */
    int getPartitionCount(String topic);

    /**
     * <p>Title: shutDown</p>
     * <p>Description: shutDown this receiver</p>
     */
    void shutDown();
}
