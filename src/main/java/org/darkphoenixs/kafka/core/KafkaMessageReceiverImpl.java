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

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.BrokerEndPoint;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;
import org.darkphoenixs.kafka.pool.KafkaMessageReceiverPool;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.util.RefleTool;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>Title: KafkaMessageReceiverImpl</p>
 * <p>Description: Kafka消息接收实现类</p>
 *
 * @author Victor.Zxy
 * @version 1.0
 * @see KafkaMessageReceiver
 * @since 2015-06-01
 */
public class KafkaMessageReceiverImpl<K, V> implements
        KafkaMessageReceiver<K, V> {

    /**
     * consumer
     */
    private final AtomicReference<SimpleConsumer> consumer = new AtomicReference<SimpleConsumer>();
    /**
     * replicaBrokers
     */
    protected Map<String, Integer> replicaBrokers;
    /**
     * metadata
     */
    protected PartitionMetadata metadata;
    /**
     * fetchResponse
     */
    protected FetchResponse fetchResponse;
    /**
     * pool
     */
    private KafkaMessageReceiverPool<K, V> pool;
    /**
     * props
     */
    private VerifiableProperties props;

    /**
     * Construction method.
     *
     * @param props param props
     * @param pool  receiver pool
     */
    public KafkaMessageReceiverImpl(Properties props,
                                    KafkaMessageReceiverPool<K, V> pool) {

        this.pool = pool;
        this.props = new VerifiableProperties(props);
        this.replicaBrokers = new LinkedHashMap<String, Integer>();
    }

    @Override
    public synchronized List<V> receive(String topic, int partition, long beginOffset,
                                        long readOffset) {
        if (readOffset <= 0) {

            throw new IllegalArgumentException("read offset must be greater than 0");
        }

        List<V> messages = new ArrayList<V>();

        boolean returnFlag = false;

        for (int i = 0; i < 3; i++) {

            if (checkLeader(topic, partition, beginOffset)) {
                returnFlag = true;
                break;
            }
        }

        if (!returnFlag)
            return messages;

        for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(
                topic, partition)) {

            long currentOffset = messageAndOffset.offset();

            if (currentOffset > beginOffset + readOffset - 1) {

                break;
            }

            ByteBuffer valload = messageAndOffset.message().payload();

            byte[] vals = new byte[valload.limit()];

            valload.get(vals);

            @SuppressWarnings("unchecked")
            Decoder<V> decoder = (Decoder<V>) RefleTool.newInstance(pool.getValDecoderClass(), props);

            V val = decoder.fromBytes(vals);

            messages.add(val);
        }

        return messages;
    }

    @Override
    public synchronized Map<K, V> receiveWithKey(String topic, int partition,
                                                 long beginOffset, long readOffset) {
        if (readOffset <= 0) {

            throw new IllegalArgumentException("read offset must be greater than 0");
        }

        Map<K, V> messages = new LinkedHashMap<K, V>();

        boolean returnFlag = false;

        for (int i = 0; i < 3; i++) {

            if (checkLeader(topic, partition, beginOffset)) {
                returnFlag = true;
                break;
            }
        }

        if (!returnFlag)
            return messages;

        for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(
                topic, partition)) {

            long currentOffset = messageAndOffset.offset();

            if (currentOffset > beginOffset + readOffset - 1) {

                break;
            }

            ByteBuffer keyload = messageAndOffset.message().key();

            ByteBuffer valload = messageAndOffset.message().payload();

            byte[] keys = new byte[keyload.limit()];
            byte[] vals = new byte[valload.limit()];

            keyload.get(keys);
            valload.get(vals);

            @SuppressWarnings("unchecked")
            Decoder<K> keyDecoder = (Decoder<K>) RefleTool.newInstance(pool.getKeyDecoderClass(), props);
            @SuppressWarnings("unchecked")
            Decoder<V> valDecoder = (Decoder<V>) RefleTool.newInstance(pool.getValDecoderClass(), props);

            K key = keyDecoder.fromBytes(keys);
            V val = valDecoder.fromBytes(vals);

            messages.put(key, val);

        }

        return messages;
    }

    @Override
    public synchronized long getLatestOffset(String topic, int partition) {

        if (checkConsumer(topic, partition)) {

            TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
                    partition);
            Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
            requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
                    kafka.api.OffsetRequest.LatestTime(), 1));
            kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                    requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
                    pool.getClientId());
            OffsetResponse response = consumer.get().getOffsetsBefore(request);

            if (response.hasError()) {
                logger.error("Error fetching data Offset Data the Broker. Reason: "
                        + response.errorCode(topic, partition));
                return 0;
            }
            long[] offsets = response.offsets(topic, partition);
            return offsets[0];
        }
        return -1;
    }

    @Override
    public synchronized long getEarliestOffset(String topic, int partition) {

        if (checkConsumer(topic, partition)) {

            TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
                    partition);
            Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
            requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
                    kafka.api.OffsetRequest.EarliestTime(), 1));
            kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                    requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
                    pool.getClientId());
            OffsetResponse response = consumer.get().getOffsetsBefore(request);

            if (response.hasError()) {
                logger.error("Error fetching data Offset Data the Broker. Reason: "
                        + response.errorCode(topic, partition));
                return 0;
            }
            long[] offsets = response.offsets(topic, partition);
            return offsets[0];
        }

        return -1;
    }

    @Override
    public int getPartitionCount(String topic) {

        return getPartitionNum(topic);
    }

    @Override
    public synchronized void shutDown() {

        if (this.consumer.get() != null) {

            this.consumer.get().close();

            this.consumer.set(null);
        }
    }

    /**
     * Find new leader
     *
     * @param a_oldLeader oldleader host
     * @param a_topic     topic name
     * @param a_partition partition number
     * @return PartitionMetadata
     * @throws MQException
     */
    private PartitionMetadata findNewLeader(String a_oldLeader, String a_topic,
                                            int a_partition) throws MQException {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(replicaBrokers, a_topic,
                    a_partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host())
                    && i == 0) {
                // first time through if the leader hasn't changed give
                // ZooKeeper a second to recover
                // second time, assume the broker did recover before failover,
                // or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return metadata;
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        logger.error("Unable to find new leader after Broker failure. Exiting");
        throw new MQException(
                "Unable to find new leader after Broker failure. Exiting");
    }

    /**
     * Find the leader
     *
     * @param a_seedBrokers seed brokers
     * @param a_topic       topic name
     * @param a_partition   partition number
     * @return PartitionMetadata
     */
    private PartitionMetadata findLeader(Map<String, Integer> a_seedBrokers,
                                         String a_topic, int a_partition) {

        PartitionMetadata returnMetaData = null;

        for (Entry<String, Integer> entry : a_seedBrokers.entrySet()) {

            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(entry.getKey(), entry.getValue(),
                        KafkaConstants.SO_TIMEOUT, KafkaConstants.BUFFER_SIZE,
                        "leaderLookup");
                List<String> topics = Collections.singletonList(a_topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == a_partition) {
                            returnMetaData = part;
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Error communicating with Broker ["
                        + entry.getKey() + "] to find Leader for [" + a_topic
                        + ", " + a_partition + "] Reason: " + e);
            } finally {
                if (consumer != null)
                    consumer.close();
            }
        }

        if (returnMetaData != null) {
            replicaBrokers.clear();
            for (BrokerEndPoint replica : returnMetaData.replicas()) {
                replicaBrokers.put(replica.host(), replica.port());
            }
        }
        return returnMetaData;
    }

    /**
     * Check the leader.
     *
     * @param a_topic       topic name
     * @param a_partition   partition number
     * @param a_beginOffset begin offset
     * @return boolean
     */
    private boolean checkLeader(String a_topic, int a_partition,
                                long a_beginOffset) {

        if (checkConsumer(a_topic, a_partition)) {

            FetchRequest req = new FetchRequestBuilder()
                    .clientId(pool.getClientId())
                    .addFetch(a_topic, a_partition, a_beginOffset,
                            KafkaConstants.FETCH_SIZE).build();
            fetchResponse = consumer.get().fetch(req);
            String leadHost = metadata.leader().host();

            if (fetchResponse.hasError()) {

                // Something went wrong!
                short code = fetchResponse.errorCode(a_topic, a_partition);
                logger.error("Error fetching data from the Broker:" + leadHost
                        + " Reason: " + code);

                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    // We asked for an invalid offset. For simple case ask for
                    // the last element to reset
                    a_beginOffset = getLatestOffset(a_topic, a_partition);
                }
                consumer.get().close();
                consumer.set(null);

                try {
                    metadata = findNewLeader(leadHost, a_topic, a_partition);
                } catch (MQException e) {
                    logger.error("Find new leader failed.", e);
                }
                return false;
            }

            return true;
        }
        return false;
    }

    /**
     * Check the consumer and create consumer.
     *
     * @param a_topic     topic name
     * @param a_partition partition number
     * @return boolean
     */
    private boolean checkConsumer(String a_topic, int a_partition) {

        if (consumer.get() == null) {

            if (metadata == null) {

                replicaBrokers.clear();
                String brokerStr = getBrokerStr(a_topic);
                String[] brokers = brokerStr.split(",");
                for (String broker : brokers) {
                    String[] hostport = broker.split(":");
                    replicaBrokers.put(hostport[0],
                            Integer.valueOf(hostport[1]));
                }
                metadata = findLeader(replicaBrokers, a_topic, a_partition);
            }

            if (metadata == null) {
                logger.error("Can't find metadata for Topic and Partition. Exiting");
                return false;
            }
            if (metadata.leader() == null) {
                logger.error("Can't find Leader for Topic and Partition. Exiting");
                return false;
            }
            String leadHost = metadata.leader().host();
            Integer leadPort = metadata.leader().port();
            String clientName = pool.getClientId();

            consumer.set(new SimpleConsumer(leadHost, leadPort,
                    KafkaConstants.SO_TIMEOUT, KafkaConstants.BUFFER_SIZE,
                    clientName));
        }

        return true;
    }

    /**
     * Get broker address
     *
     * @param topic topic name
     * @return broker address
     */
    private String getBrokerStr(String topic) {

        ZookeeperHosts zkHosts = new ZookeeperHosts(pool.getZookeeperStr(), topic);
        ZookeeperBrokers brokers = new ZookeeperBrokers(zkHosts);
        String brokerStr = brokers.getBrokerInfo();
        brokers.close();
        return brokerStr;
    }

    /**
     * Get partition number
     *
     * @param topic topic name
     * @return partition number
     */
    private int getPartitionNum(String topic) {

        ZookeeperHosts zkHosts = new ZookeeperHosts(pool.getZookeeperStr(), topic);
        ZookeeperBrokers brokers = new ZookeeperBrokers(zkHosts);
        int partitionNum = brokers.getNumPartitions();
        brokers.close();
        return partitionNum;
    }

}
