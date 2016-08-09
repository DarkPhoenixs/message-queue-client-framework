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

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.darkphoenixs.kafka.core.KafkaConstants;
import org.darkphoenixs.kafka.core.KafkaMessageAdapter;
import org.darkphoenixs.kafka.core.KafkaMessageNewReceiver;
import org.darkphoenixs.kafka.core.KafkaMessageReceiver;
import org.darkphoenixs.mq.exception.MQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>Title: KafkaMessageNewReceiverPool</p>
 * <p>Description: 新Kafka消息接收线程池</p>
 * <p>
 * <p>采用两种设计模式</p>
 * <li>模式一：数据接收与业务处理在同一线程中（并发取决于队列分区）</li>
 * <li>模式二：接收线程与业务线程分离（类Reactor模式）</li>
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
     * The Receiver pool.
     */
    protected ExecutorService receivPool;
    /**
     * The Handler pool.
     */
    protected ExecutorService handlePool;
    /**
     * The Threads.
     */
    protected List<ReceiverThread> threads = new ArrayList<ReceiverThread>();
    /**
     * The Model.
     * <p>
     * Default MODEL_1.
     */
    private MODEL model = MODEL.MODEL_1;
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
     * When MODEL is MODEL_1, the size is the consumer thread pool size.
     * <p>
     * When MODEL is MODEL_2, the size is the handle thread pool size,
     * the consumer thread pool size as same as the topic partition number.
     */
    private int poolSize;
    /**
     * messageAdapter
     */
    private KafkaMessageAdapter<?, ?> messageAdapter;

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
            logger.error(e.getMessage());
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
     */
    public void setModel(String model) {

        this.model = MODEL.valueOf(model);
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
    }

    @Override
    public KafkaMessageReceiver<K, V> getReceiver() {

        return new KafkaMessageNewReceiver<K, V>(props);
    }

    @Override
    public void returnReceiver(KafkaMessageReceiver<K, V> receiver) {

        if (receiver != null)

            receiver.shutDown();
    }

    @Override
    public synchronized void init() {

        String topic = messageAdapter.getDestination().getDestinationName();

        KafkaMessageReceiver<K, V> receiver = getReceiver();

        // partition size
        int partSize = receiver.getPartitionCount(topic);

        returnReceiver(receiver);

        switch (model) {

            case MODEL_1: // MODEL_1

                if (poolSize == 0 || poolSize > partSize)

                    // default partition size
                    setPoolSize(partSize);

                receivPool = Executors.newFixedThreadPool(poolSize, new KafkaPoolThreadFactory(ReceiverThread.tagger + "-" + topic));

                logger.info("Kafka Consumer config : " + props.toString());

                logger.info("Message Receiver Pool initializing. poolSize : " + poolSize);

                for (int i = 0; i < poolSize; i++) {

                    ReceiverThread receiverThread = new ReceiverThread(props, topic, messageAdapter);

                    threads.add(receiverThread);

                    receivPool.submit(receiverThread);
                }

                break;

            case MODEL_2: // MODEL_2

                receivPool = Executors.newFixedThreadPool(partSize, new KafkaPoolThreadFactory(ReceiverThread.tagger + "-" + topic));

                handlePool = Executors.newFixedThreadPool(poolSize, new KafkaPoolThreadFactory(HandlerThread.tagger + "-" + topic));

                logger.info("Kafka Consumer config : " + props.toString());

                logger.info("Message Receiver Pool initializing poolSize : " + partSize);

                logger.info("Message Handler Pool initializing poolSize : " + poolSize);

                for (int i = 0; i < partSize; i++) {

                    ReceiverThread receiverThread = new ReceiverThread(props, topic, messageAdapter);

                    threads.add(receiverThread);

                    receivPool.submit(receiverThread);
                }

                break;
        }

    }

    @Override
    public synchronized void destroy() {

        if (handlePool != null)

            handlePool.shutdown();

        for (ReceiverThread thread : threads)

            thread.shutdown();

        if (receivPool != null)

            receivPool.shutdown();
    }

    /**
     * The enum Model.
     */
    public enum MODEL {

        /**
         * Model 1 model.
         */
        MODEL_1,
        /**
         * Model 2 model.
         */
        MODEL_2;
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

                    ConsumerRecords<K, V> records = consumer.poll(KafkaConstants.MAX_POLL_TIMEOUT);

                    // Handle new records
                    switch (model) {

                        case MODEL_1:

                            try {
                                adapter.messageAdapter(records);

                            } catch (MQException e) {

                                logger.error(Thread.currentThread().getName() + " Exception: " + e.getMessage());
                            }

                            break;

                        case MODEL_2:

                            handlePool.execute(new HandlerThread(adapter, records));

                            break;
                    }
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

        private final KafkaMessageAdapter<?, ?> adapter;

        private final ConsumerRecords<K, V> records;

        /**
         * Instantiates a new Handler thread.
         *
         * @param adapter the adapter
         * @param records the records
         */
        public HandlerThread(KafkaMessageAdapter<?, ?> adapter, ConsumerRecords<K, V> records) {

            this.adapter = adapter;

            this.records = records;
        }

        @Override
        public void run() {

            logger.info(Thread.currentThread().getName() + " start.");

            try {
                adapter.messageAdapter(records);

            } catch (MQException e) {

                logger.error(Thread.currentThread().getName() + " Exception: " + e.getMessage());
            }

            logger.info(Thread.currentThread().getName() + " end.");
        }
    }
}
