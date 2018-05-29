/*
 * Copyright (c) 2016. Dark Phoenixs (Open-Source Organization).
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

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.darkphoenixs.kafka.core.*;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.util.MQ_BATCH;
import org.darkphoenixs.mq.util.MQ_MODEL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>Title: KafkaMessageNewReceiverPool</p>
 * <p>Description: 新Kafka消息接收线程池</p>
 * <p>
 * <p>采用两种设计模式</p>
 * <li>模式一：数据接收与业务处理在同一线程中（并发取决于队列分区）</li>
 * <li>模式二：接收线程与业务线程分离（异步处理数据）</li>
 *
 * @param <K> the type of kafka message key
 * @param <V> the type of kafka message value
 * @author Victor.Zxy
 * @version 1.4.0
 * @see MessageReceiverPool
 * @since 2016 /7/27
 */
public class KafkaMessageNewReceiverPool<K, V> implements MessageReceiverPool<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageNewReceiverPool.class);

    /**
     * The enum Commit.
     */
    public enum COMMIT {

        /**
         * auto commit.
         */
        AUTO_COMMIT,
        /**
         * sync commit.
         */
        SYNC_COMMIT,
        /**
         * async commit.
         */
        ASYNC_COMMIT
    }

    /**
     * The blocking queue.
     */
    protected BlockingQueue<ConsumerRecords<K, V>> blockingQueue;
    /**
     * The Receiver pool.
     */
    protected ExecutorService receivPool;
    /**
     * The Handler pool.
     */
    protected ExecutorService handlePool;
    /**
     * The ReceiverThreads.
     */
    protected List<ReceiverThread> receivThreads = new ArrayList<ReceiverThread>();
    /**
     * The HandleThreads.
     */
    protected List<HandlerThread> handleThreads = new ArrayList<HandlerThread>();
    /**
     * The Running.
     */
    protected AtomicBoolean running = new AtomicBoolean(false);

    /**
     * The Model.
     * <p>
     * Default MODEL_1.
     */
    private MQ_MODEL model = MQ_MODEL.MODEL_1;
    /**
     * The Batch.
     * <p>
     * Default NON_BATCH.
     */
    private MQ_BATCH batch = MQ_BATCH.NON_BATCH;
    /**
     * The Commit.
     * <p>
     * Default AUTO_COMMIT.
     */
    private COMMIT commit = COMMIT.AUTO_COMMIT;
    /**
     * The Props.
     */
    private Properties props = new Properties();
    /**
     * The Config.
     */
    private Resource config;
    /**
     * The Pool size.
     * <p>
     * The size is the consumer thread pool size.
     */
    private int poolSize;
    /**
     * How many multiple is the consumer thread pool size, MODEL_2 to take effect.
     * <p>
     * When MODEL is MODEL_2, the handle thread pool size is (poolSize * handleMultiple + 1).
     */
    private int handleMultiple = 2;
    /**
     * The message receive retry Count.
     * <p>
     * When MQ_BATCH is NON_BATCH to take effect.
     */
    private int retryCount = 3;
    /**
     * The Blocking queue size.
     * <p>
     * When MODEL is MODEL_2 to take effect.
     */
    private int queueSize = 100000;
    /**
     * The Thread sleep time(ms).
     * <p>
     * To prevent the CPU usage is too high.
     */
    private long threadSleep = 0;

    /**
     * The Kafka poll timeout time(ms).
     * <p>
     * Default 2000ms.
     */
    private long pollTimeout = 2000;

    /**
     * messageAdapter
     */
    private KafkaMessageAdapter<?, ?> messageAdapter;

    /**
     * destination
     */
    private KafkaDestination destination;

    /**
     * receiverRetry
     */
    private KafkaMessageReceiverRetry<ConsumerRecord<K, V>> receiverRetry;

    /**
     * Gets props.
     *
     * @return the props
     */
    public Properties getProps() {
        return props;
    }

    /**
     * Sets props.
     *
     * @param props the props
     */
    public void setProps(Properties props) {
        this.props = props;
    }

    /**
     * Gets handle multiple.
     *
     * @return the handle multiple
     */
    public int getHandleMultiple() {
        return handleMultiple;
    }

    /**
     * Sets handle multiple.
     *
     * @param handleMultiple the handle multiple
     */
    public void setHandleMultiple(int handleMultiple) {
        this.handleMultiple = handleMultiple;
    }

    /**
     * Gets retry count.
     *
     * @return the retry count
     */
    public int getRetryCount() {
        return retryCount;
    }

    /**
     * Sets retry count.
     *
     * @param retryCount the retry count
     */
    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    /**
     * Gets pool size.
     *
     * @return the pool size
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * Sets pool size.
     *
     * @param poolSize the pool size
     */
    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    /**
     * Gets queue size.
     *
     * @return the queue size
     */
    public int getQueueSize() {
        return queueSize;
    }

    /**
     * Sets queue size.
     *
     * @param queueSize the queue size
     */
    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    /**
     * Gets thread sleep.
     *
     * @return the thread sleep
     */
    public long getThreadSleep() {
        return threadSleep;
    }

    /**
     * Sets thread sleep.
     *
     * @param threadSleep the thread sleep
     */
    public void setThreadSleep(long threadSleep) {
        this.threadSleep = threadSleep;
    }

    /**
     * Gets poll timeout.
     *
     * @return the poll timeout
     */
    public long getPollTimeout() {
        return pollTimeout;
    }

    /**
     * Sets poll timeout.
     *
     * @param pollTimeout the poll timeout
     */
    public void setPollTimeout(long pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    /**
     * Gets config.
     *
     * @return the config
     */
    public Resource getConfig() {
        return config;
    }

    /**
     * Sets config.
     *
     * @param config the config
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
     * Gets model.
     *
     * @return the model
     */
    public String getModel() {

        return model.name();
    }

    /**
     * Sets model.
     *
     * @param model the model
     * @see KafkaMessageAdapter#setModel(String model)
     */
    @Deprecated
    public void setModel(String model) {

        this.model = MQ_MODEL.valueOf(model);
    }

    /**
     * Gets batch.
     *
     * @return the batch
     */
    public String getBatch() {

        return batch.name();
    }

    /**
     * Sets batch.
     *
     * @param batch the batch
     * @see KafkaMessageAdapter#setBatch(String batch)
     */
    @Deprecated
    public void setBatch(String batch) {

        this.batch = MQ_BATCH.valueOf(batch);
    }

    /**
     * Gets commit.
     *
     * @return the commit
     */
    public String getCommit() {

        return commit.name();
    }

    /**
     * Sets commit.
     *
     * @param commit the commit
     */
    public void setCommit(String commit) {

        this.commit = COMMIT.valueOf(commit);

        if (!this.commit.equals(COMMIT.AUTO_COMMIT))
            props.setProperty(KafkaConstants.ENABLE_AUTO_COMMIT, "false");
    }

    /**
     * Gets message adapter.
     *
     * @return the message adapter
     */
    public KafkaMessageAdapter<?, ?> getMessageAdapter() {
        return messageAdapter;
    }

    /**
     * Sets message adapter.
     *
     * @param messageAdapter the message adapter
     */
    public void setMessageAdapter(KafkaMessageAdapter<?, ?> messageAdapter) {
        this.messageAdapter = messageAdapter;
        if (messageAdapter.getModel() != null)
            this.setModel(messageAdapter.getModel());
        if (messageAdapter.getBatch() != null)
            this.setBatch(messageAdapter.getBatch());
        if (messageAdapter.getDestination() != null)
            this.setDestination(messageAdapter.getDestination());
    }

    /**
     * Gets client id.
     *
     * @return the client id
     */
    public String getClientId() {
        return this.props.getProperty(KafkaConstants.CLIENT_ID, "client_new_consumer");
    }

    /**
     * Gets group id.
     *
     * @return the group id
     */
    public String getGroupId() {
        return this.props.getProperty(KafkaConstants.GROUP_ID, "group_new_consumer");
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

    @Override
    public KafkaMessageReceiver<K, V> getReceiver() {

        Properties properties = (Properties) props.clone();

        properties.setProperty(KafkaConstants.GROUP_ID, "group_new_consumer");

        properties.setProperty(KafkaConstants.CLIENT_ID, "client_new_consumer");

        return new KafkaMessageNewReceiver<K, V>(properties);
    }

    @Override
    public void returnReceiver(KafkaMessageReceiver<K, V> receiver) {

        if (receiver != null)

            receiver.shutDown();
    }

    @Override
    public synchronized void init() {

        String topic = destination.getDestinationName();

        KafkaMessageReceiver<K, V> receiver = getReceiver();

        // partition size
        int partSize = receiver.getPartitionCount(topic);

        if (poolSize == 0 || poolSize > partSize)
            // pool size default partition size
            setPoolSize(partSize);

        returnReceiver(receiver);

        if (retryCount > 0 && batch.equals(MQ_BATCH.NON_BATCH))
            // retry count > 0 and batch is NON_BATCH
            receiverRetry = new KafkaMessageReceiverRetry<ConsumerRecord<K, V>>(topic, retryCount, messageAdapter);

        switch (model) {

            case MODEL_1: // MODEL_1

                receivPool = Executors.newFixedThreadPool(poolSize, new KafkaPoolThreadFactory(ReceiverThread.tagger + "-" + topic));

                break;

            case MODEL_2: // MODEL_2

                int handSize = poolSize * handleMultiple + 1;

                blockingQueue = new LinkedBlockingQueue<ConsumerRecords<K, V>>(queueSize);

                receivPool = Executors.newFixedThreadPool(poolSize, new KafkaPoolThreadFactory(ReceiverThread.tagger + "-" + topic));

                handlePool = Executors.newFixedThreadPool(handSize, new KafkaPoolThreadFactory(HandlerThread.tagger + "-" + topic));

                for (int i = 0; i < handSize; i++) {

                    HandlerThread handlerThread = new HandlerThread(messageAdapter);

                    handleThreads.add(handlerThread);

                    handlePool.submit(handlerThread);
                }

                logger.info("Message Handler Pool initialized. PoolSize : " + handSize);

                break;
        }

        for (int i = 0; i < poolSize; i++) {

            Properties properties = (Properties) props.clone();

            properties.setProperty(KafkaConstants.CLIENT_ID, getClientId() + "-" + topic + "-" + i);

            ReceiverThread receiverThread = new ReceiverThread(properties, topic, messageAdapter);

            receivThreads.add(receiverThread);

            receivPool.submit(receiverThread);
        }

        logger.info("Message Receiver Pool initialized. PoolSize : " + poolSize);

        running.set(true);
    }

    @Override
    public synchronized void destroy() {

        for (ReceiverThread thread : receivThreads)

            thread.shutdown();

        if (receivPool != null) {

            receivPool.shutdown();

            logger.info("Message Receiver pool closed.");
        }

        if (blockingQueue != null)

            while (!blockingQueue.isEmpty()) ;

        for (HandlerThread thread : handleThreads)

            thread.shutdown();

        if (handlePool != null) {

            handlePool.shutdown();

            logger.info("Message Handler pool closed.");
        }

        if (receiverRetry != null) {

            receiverRetry.destroy();
        }

        running.set(false);
    }

    @Override
    public synchronized boolean isRunning() {

        return running.get();
    }

    /**
     * The type Receiver thread.
     */
    class ReceiverThread implements Runnable {

        /**
         * The constant tagger.
         */
        public static final String tagger = "ReceiverThread";

        private final AtomicBoolean closed = new AtomicBoolean(false);

        private final KafkaConsumer<K, V> consumer;

        private final KafkaMessageAdapter<?, ?> adapter;

        private final String topic;

        /**
         * Instantiates a new Receiver thread.
         *
         * @param props   the props
         * @param topic   the topic
         * @param adapter the adapter
         */
        public ReceiverThread(Properties props, String topic, KafkaMessageAdapter<?, ?> adapter) {

            this.topic = topic;

            this.adapter = adapter;

            consumer = new KafkaConsumer<K, V>(props);
        }

        @Override
        public void run() {

            logger.info(Thread.currentThread().getName() + " start.");

            try {
                consumer.subscribe(Arrays.asList(topic));

                while (!closed.get()) {

                    ConsumerRecords<K, V> records = consumer.poll(pollTimeout);

                    // Handle new records
                    switch (model) {

                        case MODEL_1: // 模式1

                            switch (batch) {

                                case BATCH: // 批量

                                    try {
                                        adapter.messageAdapter(records);

                                    } catch (MQException e) {

                                        logger.error("Receive message failed. failSize:" + records.count(), e);

                                    } finally {

                                        batchCommit(consumer, commit); // 批量提交
                                    }

                                    break;

                                case NON_BATCH: // 非批量

                                    for (ConsumerRecord<K, V> record : records)

                                        try {
                                            adapter.messageAdapter(record);

                                        } catch (MQException e) {

                                            messageReceiveRetry(record); // 消息重试

                                            logger.error("Receive message failed."
                                                    + " topic: " + record.topic()
                                                    + " offset: " + record.offset()
                                                    + " partition: " + record.partition(), e);
                                        } finally {

                                            commit(consumer, record, commit); // 逐个提交
                                        }

                                    break;
                            }

                            break;

                        case MODEL_2:

                            try {
                                blockingQueue.put(records);

                            } catch (InterruptedException e) {

                                logger.error("BlockingQueue put failed.", e);
                            }

                            batchCommit(consumer, commit); // 批量提交

                            break;
                    }

                    waitAmoment(threadSleep);
                }

            } catch (WakeupException e) {
                // Ignore exception if closing
                if (!closed.get()) throw e;

            } finally {

                consumer.close();
            }

            logger.info(Thread.currentThread().getName() + " end.");
        }

        /**
         * Shutdown hook which can be called from a separate thread.
         */
        public void shutdown() {

            closed.set(true);

            consumer.wakeup();
        }
    }

    /**
     * The type Handler thread.
     */
    class HandlerThread implements Runnable {

        /**
         * The constant tagger.
         */
        public static final String tagger = "HandlerThread";

        private final AtomicBoolean closed = new AtomicBoolean(false);

        private final KafkaMessageAdapter<?, ?> adapter;

        /**
         * Instantiates a new Handler thread.
         *
         * @param adapter the adapter
         */
        public HandlerThread(KafkaMessageAdapter<?, ?> adapter) {

            this.adapter = adapter;
        }

        @Override
        public void run() {

            logger.info(Thread.currentThread().getName() + " start.");

            while (!closed.get()) {

                ConsumerRecords<K, V> records = null;

                try {
                    records = blockingQueue.take();

                } catch (InterruptedException e) {

                    logger.error("BlockingQueue take failed.", e);
                }

                switch (batch) {

                    case BATCH:

                        try {
                            adapter.messageAdapter(records);

                        } catch (MQException e) {

                            logger.error("Receive message failed. failSize: " + records.count(), e);
                        }

                        break;

                    case NON_BATCH:

                        for (ConsumerRecord<K, V> record : records)

                            try {
                                adapter.messageAdapter(record);

                            } catch (MQException e) {

                                messageReceiveRetry(record);

                                logger.error("Receive message failed."
                                        + " topic: " + record.topic()
                                        + " offset: " + record.offset()
                                        + " partition: " + record.partition(), e);
                            }

                        break;
                }

                waitAmoment(threadSleep);
            }

            logger.info(Thread.currentThread().getName() + " end.");
        }

        /**
         * Shutdown hook which can be called from a separate thread.
         */
        public void shutdown() {

            closed.set(true);
        }
    }

    /**
     * Commit offSet.
     *
     * @param consumer consumer
     * @param record   record
     * @param commit   commit
     */
    private void commit(KafkaConsumer<K, V> consumer, ConsumerRecord<K, V> record, COMMIT commit) {

        switch (commit) {

            case SYNC_COMMIT:
                consumer.commitSync(Collections.singletonMap(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)));
                break;
            case ASYNC_COMMIT:
                consumer.commitAsync(Collections.singletonMap(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)),
                        offsetCommitCallback);
                break;
            default:
                break;
        }
    }

    /**
     * Batch Commit.
     *
     * @param consumer consumer
     * @param commit   commit
     */
    private void batchCommit(KafkaConsumer<K, V> consumer, COMMIT commit) {

        switch (commit) {

            case SYNC_COMMIT: // 同步提交
                consumer.commitSync();
                break;
            case ASYNC_COMMIT: // 异步提交
                consumer.commitAsync();
                break;
            default:
                break;
        }
    }

    /**
     * ConsumerRecord.
     *
     * @param consumerRecord
     */
    private void messageReceiveRetry(ConsumerRecord<K, V> consumerRecord) {

        if (receiverRetry != null)

            receiverRetry.receiveMessageRetry(consumerRecord);
    }

    /**
     * Wait a moment.
     *
     * @param ms millisecond
     */
    private void waitAmoment(long ms) {

        if (ms > 0) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * The Offset commit callback.
     */
    protected OffsetCommitCallback offsetCommitCallback = new OffsetCommitCallback() {

        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                logger.error("Offset commit with offsets {} failed", offsets, exception);
        }
    };
}
