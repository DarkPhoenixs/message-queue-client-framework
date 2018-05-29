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
package org.darkphoenixs.kafka.pool;

import kafka.common.OffsetAndMetadata;
import kafka.common.TopicAndPartition;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;
import org.darkphoenixs.kafka.core.*;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.util.RefleTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>Title: KafkaMessageReceiverPool</p>
 * <p>Description: Kafka消息接受线程池</p>
 *
 * @author Victor.Zxy
 * @version 1.0
 * @since 2015-06-01
 */
public class KafkaMessageReceiverPool<K, V> implements MessageReceiverPool<K, V> {

    private static final String tagger = "KafkaMessageReceiverPool";

    private static final Logger logger = LoggerFactory
            .getLogger(KafkaMessageReceiverPool.class);

    /**
     * consumer
     */
    protected ConsumerConnector consumer;
    /**
     * pool
     */
    protected ExecutorService pool;
    /**
     * props
     */
    protected Properties props = new Properties();
    /**
     * The Running.
     */
    protected AtomicBoolean running = new AtomicBoolean(false);

    /**
     * messageAdapter
     */
    private KafkaMessageAdapter<?, ?> messageAdapter;

    /**
     * destination
     */
    private KafkaDestination destination;

    /**
     * poolSize
     */
    private int poolSize;
    /**
     * config
     */
    private Resource config;
    /**
     * retryCount
     */
    private int retryCount = 3;
    /**
     * receiverRetry
     */
    private KafkaMessageReceiverRetry<MessageAndMetadata<K, V>> receiverRetry;
    /**
     * keyDecoder
     */
    private Class<?> keyDecoderClass = DefaultDecoder.class;
    /**
     * valDecoder
     */
    private Class<?> valDecoderClass = DefaultDecoder.class;

    /**
     * threadFactory
     */
    private ThreadFactory threadFactory;

    /**
     * Init threadFactory.
     */
    public KafkaMessageReceiverPool() {

    }

    /**
     * @return the threadFactory
     */
    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    /**
     * @param threadFactory the threadFactory to set
     */
    public void setThreadFactory(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
    }

    /**
     * @return the clientId
     */
    public String getClientId() {
        return props.getProperty(KafkaConstants.CLIENT_ID);
    }

    /**
     * @param clientId the clientId to set
     */
    public void setClientId(String clientId) {
        props.setProperty(KafkaConstants.CLIENT_ID, clientId);
    }

    /**
     * Gets destination.
     *
     * @return the destination
     */
    public KafkaDestination getDestination() {
        return destination;
    }

    /**
     * Sets destination.
     *
     * @param destination the destination
     */
    public void setDestination(KafkaDestination destination) {
        this.destination = destination;
    }

    /**
     * @return the zookeeperStr
     */
    public String getZookeeperStr() {
        return props.getProperty(KafkaConstants.ZOOKEEPER_LIST);
    }

    /**
     * @param zookeeperStr the zookeeperStr to set
     */
    public void setZookeeperStr(String zookeeperStr) {
        props.setProperty(KafkaConstants.ZOOKEEPER_LIST, zookeeperStr);
    }

    /**
     * @return the autoCommit
     */
    public Boolean getAutoCommit() {
        return Boolean.valueOf(props.getProperty(KafkaConstants.AUTO_COMMIT_ENABLE, "true"));
    }

    /**
     * @param autoCommit the autoCommit to set
     */
    public void setAutoCommit(boolean autoCommit) {
        props.setProperty(KafkaConstants.AUTO_COMMIT_ENABLE,
                String.valueOf(autoCommit));
    }

    /**
     * @return the retryCount
     */
    public int getRetryCount() {
        return retryCount;
    }

    /**
     * Note: AutoCommit is false to take effect.
     *
     * @param retryCount the retryCount to set
     */
    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    /**
     * @return the props
     */
    public Properties getProps() {
        return props;
    }

    /**
     * @param props the props to set
     */
    public void setProps(Properties props) {
        this.props = props;
    }

    /**
     * @return the poolSize
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * @param poolSize the poolSize to set
     */
    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    /**
     * @return the config
     */
    public Resource getConfig() {
        return config;
    }

    /**
     * @param config the config to set
     */
    public void setConfig(Resource config) {
        this.config = config;
        try {
            PropertiesLoaderUtils.fillProperties(props, this.config);
        } catch (IOException e) {
            logger.error("Fill properties failed.", e);
        }
    }

    /**
     * @return the keyDecoderClass
     */
    public Class<?> getKeyDecoderClass() {
        return keyDecoderClass;
    }

    /**
     * @param keyDecoderClass the keyDecoderClass to set
     */
    public void setKeyDecoderClass(Class<?> keyDecoderClass) {
        this.keyDecoderClass = keyDecoderClass;
    }

    /**
     * @return the valDecoderClass
     */
    public Class<?> getValDecoderClass() {
        return valDecoderClass;
    }

    /**
     * @param valDecoderClass the valDecoder to set
     */
    public void setValDecoderClass(Class<?> valDecoderClass) {
        this.valDecoderClass = valDecoderClass;
    }

    /**
     * @return the messageAdapter
     */
    public KafkaMessageAdapter<?, ?> getMessageAdapter() {
        return messageAdapter;
    }

    /**
     * @param messageAdapter the messageAdapter to set
     */
    public void setMessageAdapter(KafkaMessageAdapter<?, ?> messageAdapter) {
        this.messageAdapter = messageAdapter;
        if (messageAdapter.getDestination() != null)
            this.setDestination(messageAdapter.getDestination());
    }

    /**
     * Get a receiver from the pool (just only create a lower-level receiver).
     *
     * @return a receiver instance
     */
    @Override
    public KafkaMessageReceiver<K, V> getReceiver() {

        KafkaMessageReceiver<K, V> receiver = new KafkaMessageReceiverImpl<K, V>(
                props, this);

        return receiver;
    }

    /**
     * Return a receiver back to pool.
     */
    @Override
    public void returnReceiver(KafkaMessageReceiver<K, V> receiver) {

        if (receiver != null)

            receiver.shutDown();
    }

    @Override
    public synchronized void init() {

        String topic = destination.getDestinationName();

        int defaultSize = getReceiver().getPartitionCount(topic);

        if (poolSize == 0 || poolSize > defaultSize)

            setPoolSize(defaultSize);

        if (retryCount > 0)

            receiverRetry = new KafkaMessageReceiverRetry<MessageAndMetadata<K, V>>(topic, retryCount, messageAdapter);

        this.threadFactory = new KafkaPoolThreadFactory(tagger + "-" + topic);

        this.pool = Executors.newFixedThreadPool(poolSize, threadFactory);

        logger.info("Message receiver pool initializing. poolSize : "
                + poolSize + " config : " + props.toString());

        consumer = kafka.consumer.Consumer
                .createJavaConsumerConnector(new ConsumerConfig(props));

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

        topicCountMap.put(topic, poolSize);

        VerifiableProperties verProps = new VerifiableProperties(props);

        @SuppressWarnings("unchecked")
        Decoder<K> keyDecoder = (Decoder<K>) RefleTool.newInstance(
                keyDecoderClass, verProps);

        @SuppressWarnings("unchecked")
        Decoder<V> valDecoder = (Decoder<V>) RefleTool.newInstance(
                valDecoderClass, verProps);

        Map<String, List<KafkaStream<K, V>>> consumerMap = consumer
                .createMessageStreams(topicCountMap, keyDecoder, valDecoder);

        List<KafkaStream<K, V>> streams = consumerMap.get(topic);

        for (final KafkaStream<K, V> stream : streams) {

            pool.submit(new ReceiverThread(stream, messageAdapter));
        }

        logger.info("Message receiver pool initialized.");

        running.set(true);
    }

    @Override
    public synchronized void destroy() {

        logger.info("Message receiver pool closing.");

        if (consumer != null)
            consumer.shutdown();

        if (pool != null) {
            pool.shutdown();

            try {
                if (!pool.awaitTermination(KafkaConstants.INIT_TIMEOUT_MS,
                        TimeUnit.MILLISECONDS)) {
                    logger.warn("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted during shutdown, exiting uncleanly");
            }
        }

        if (receiverRetry != null)
            receiverRetry.destroy();

        logger.info("Message receiver pool closed.");

        running.set(false);
    }

    @Override
    public synchronized boolean isRunning() {

        return running.get();
    }

    /**
     * Receiver thread to receive message.
     */
    class ReceiverThread implements Runnable {

        private KafkaStream<K, V> stream;

        private KafkaMessageAdapter<?, ?> adapter;

        public ReceiverThread(KafkaStream<K, V> stream,
                              KafkaMessageAdapter<?, ?> adapter) {

            this.stream = stream;
            this.adapter = adapter;
        }

        @Override
        public void run() {

            logger.info(Thread.currentThread().getName() + " clientId: "
                    + stream.clientId() + " start.");

            ConsumerIterator<K, V> it = stream.iterator();

            while (it.hasNext()) {

                MessageAndMetadata<K, V> messageAndMetadata = it.next();

                try {
                    this.adapter.messageAdapter(messageAndMetadata);

                } catch (MQException e) {

                    if (receiverRetry != null)

                        receiverRetry.receiveMessageRetry(messageAndMetadata);

                    logger.error("Receive message failed."
                            + " topic: " + messageAndMetadata.topic()
                            + " offset: " + messageAndMetadata.offset()
                            + " partition: " + messageAndMetadata.partition(), e);

                } finally {

                    /* commitOffsets */
                    if (!getAutoCommit()) {
                        consumer.commitOffsets(Collections.singletonMap(
                                TopicAndPartition.apply(messageAndMetadata.topic(), messageAndMetadata.partition()),
                                OffsetAndMetadata.apply(messageAndMetadata.offset() + 1)), true);
                    }
                }
            }

            logger.info(Thread.currentThread().getName() + " clientId: " + stream.clientId() + " end.");
        }

    }

}
