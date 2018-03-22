/*
 * Copyright (c) 2018. Dark Phoenixs (Open-Source Organization).
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

import kafka.message.MessageAndMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.darkphoenixs.kafka.core.KafkaMessageAdapter;
import org.darkphoenixs.mq.exception.MQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The type Kafka message receiver retry.
 *
 * @param <T> the type parameter
 */
public class KafkaMessageReceiverRetry<T> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageReceiverRetry.class);

    private final int errorPoolSize = 1;

    private final int retryCount;

    /**
     * The Error message queue.
     */
    protected final BlockingQueue<T> errorMessageQueue;

    /**
     * The Error message pool.
     */
    protected final ExecutorService errorMessagePool;

    /**
     * The Error retry threads.
     */
    protected final List<RetryThread> errorRetryThreads;

    /**
     * The Error message count.
     */
    protected final ConcurrentMap<String, AtomicInteger> errorMessageCount;

    /**
     * Instantiates a new Kafka message receiver retry.
     *
     * @param topic          the topic
     * @param retryCount     the retry count
     * @param messageAdapter the message adapter
     */
    public KafkaMessageReceiverRetry(String topic, int retryCount, KafkaMessageAdapter<?, ?> messageAdapter) {

        this.retryCount = retryCount;

        this.errorMessageQueue = new LinkedBlockingQueue<T>();

        this.errorRetryThreads = new ArrayList<RetryThread>(errorPoolSize);

        this.errorMessagePool = Executors.newFixedThreadPool(errorPoolSize, new KafkaPoolThreadFactory(KafkaMessageReceiverRetry.RetryThread.tagger + "-" + topic));

        this.errorMessageCount = new ConcurrentHashMap<String, AtomicInteger>();

        RetryThread retryThread = new RetryThread(messageAdapter);

        this.errorRetryThreads.add(retryThread);

        this.errorMessagePool.submit(retryThread);
    }

    /**
     * Receive message retry.
     *
     * @param record the record
     */
    public void receiveMessageRetry(T record) {

        try {
            if (record instanceof MessageAndMetadata) {

                MessageAndMetadata messageAndMetadata = (MessageAndMetadata) record;

                if (0 < errorMessageCount(messageAndMetadata.topic(), messageAndMetadata.partition(), messageAndMetadata.offset())) {

                    errorMessageQueue.put(record);
                }
            } else if (record instanceof ConsumerRecord) {

                ConsumerRecord consumerRecord = (ConsumerRecord) record;

                if (0 < errorMessageCount(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset())) {

                    errorMessageQueue.put(record);
                }
            }
        } catch (InterruptedException e) {

            logger.error("BlockingQueue put failed.", e);
        }
    }

    /**
     * Receive message clean.
     *
     * @param record the record
     */
    public void receiveMessageClean(T record) {

        if (record instanceof MessageAndMetadata) {

            MessageAndMetadata messageAndMetadata = (MessageAndMetadata) record;

            errorMessageCount.remove(errorMessageKey(messageAndMetadata.topic(), messageAndMetadata.partition(), messageAndMetadata.offset()));

        } else if (record instanceof ConsumerRecord) {

            ConsumerRecord consumerRecord = (ConsumerRecord) record;

            errorMessageCount.remove(errorMessageKey(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset()));
        }
    }

    /**
     * Receive message count int.
     *
     * @param record the record
     * @return the int
     */
    public int receiveMessageCount(T record) {

        if (record instanceof MessageAndMetadata) {

            MessageAndMetadata messageAndMetadata = (MessageAndMetadata) record;

            return errorMessageCount.get(errorMessageKey(messageAndMetadata.topic(), messageAndMetadata.partition(), messageAndMetadata.offset())).get();

        } else if (record instanceof ConsumerRecord) {

            ConsumerRecord consumerRecord = (ConsumerRecord) record;

            return errorMessageCount.get(errorMessageKey(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset())).get();
        }

        return 0;
    }

    /**
     * Destroy.
     */
    public void destroy() {

        if (errorMessageQueue != null)

            while (!errorMessageQueue.isEmpty()) ;

        for (RetryThread thread : errorRetryThreads)

            thread.shutdown();

        if (errorMessagePool != null) {

            errorMessagePool.shutdown();

            logger.info("Message Error pool closed.");
        }
    }

    private String errorMessageKey(final String _topic, final int _partition, final long _offset) {

        return _topic + "_" + _partition + "_" + _offset;
    }

    /**
     * Error message count int.
     *
     * @param topic     the topic
     * @param partition the partition
     * @param offset    the offset
     * @return the int
     */
    protected int errorMessageCount(String topic, int partition, long offset) {

        if (retryCount == 0)

            return 0;

        String errorKey = errorMessageKey(topic, partition, offset);

        AtomicInteger count = errorMessageCount.get(errorKey);

        if (null == count) {

            count = errorMessageCount.putIfAbsent(errorKey, new AtomicInteger(1));
        }

        if (null != count) {

            if (retryCount > count.get()) {

                return count.incrementAndGet();

            } else {
                errorMessageCount.remove(errorKey);

                return 0;
            }
        }

        return 1;
    }

    /**
     * The type Retry thread.
     */
    class RetryThread implements Runnable {

        /**
         * The constant tagger.
         */
        public static final String tagger = "RetryThread";

        private final AtomicBoolean closed = new AtomicBoolean(false);

        private final KafkaMessageAdapter<?, ?> adapter;

        /**
         * Instantiates a new Handler thread.
         *
         * @param adapter the adapter
         */
        public RetryThread(KafkaMessageAdapter<?, ?> adapter) {

            this.adapter = adapter;
        }

        @Override
        public void run() {

            logger.info(Thread.currentThread().getName() + " start.");

            while (!closed.get()) {

                T record = null;

                try {
                    record = errorMessageQueue.take();

                } catch (InterruptedException e) {

                    logger.error("BlockingQueue take failed.", e);
                }

                if (record instanceof ConsumerRecord) {

                    ConsumerRecord<?, ?> consumerRecord = (ConsumerRecord) record;

                    int retries = receiveMessageCount(record);

                    try {
                        logger.warn("Retry receive message. Number of retries: " + retries);

                        adapter.messageAdapter(consumerRecord);

                        receiveMessageClean(record);

                    } catch (MQException e) {

                        receiveMessageRetry(record);

                        logger.error("Receive message failed."
                                + " retries: " + retries
                                + " topic: " + consumerRecord.topic()
                                + " offset: " + consumerRecord.offset()
                                + " partition: " + consumerRecord.partition(), e);
                    }

                } else if (record instanceof MessageAndMetadata) {

                    MessageAndMetadata<?, ?> messageAndMetadata = (MessageAndMetadata) record;

                    int retries = receiveMessageCount(record);

                    try {
                        logger.warn("Retry receive message. Number of retries: " + retries);

                        adapter.messageAdapter(messageAndMetadata);

                        receiveMessageClean(record);

                    } catch (MQException e) {

                        receiveMessageRetry(record);

                        logger.error("Receive message failed."
                                + " retries: " + retries
                                + " topic: " + messageAndMetadata.topic()
                                + " offset: " + messageAndMetadata.offset()
                                + " partition: " + messageAndMetadata.partition(), e);
                    }
                }

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
}
