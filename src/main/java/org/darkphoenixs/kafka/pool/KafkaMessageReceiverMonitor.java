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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The type Kafka message receiver monitor.
 *
 * @param <T> the type parameter
 */
public class KafkaMessageReceiverMonitor<T> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageReceiverMonitor.class);

    private final int monitorPoolSize = 1;

    private final int monitorPercentage;

    /**
     * The Monitor pool.
     */
    protected final ScheduledExecutorService monitorPool;

    /**
     * Instantiates a new Kafka message receiver monitor.
     *
     * @param topic               the topic
     * @param monitorIntervalTime the monitor interval time
     * @param monitorPercentage   the monitor percentage
     * @param blockingQueue       the blocking queue
     */
    public KafkaMessageReceiverMonitor(String topic, long monitorIntervalTime, int monitorPercentage, BlockingQueue<T> blockingQueue) {

        this.monitorPercentage = monitorPercentage;

        this.monitorPool = Executors.newScheduledThreadPool(monitorPoolSize, new KafkaPoolThreadFactory(MonitorThread.tagger + "-" + topic));

        this.monitorPool.scheduleAtFixedRate(new MonitorThread(blockingQueue), 0, monitorIntervalTime, TimeUnit.MILLISECONDS);
    }

    /**
     * Destroy.
     */
    public void destroy() {

        if (monitorPool != null) {

            monitorPool.shutdown();

            logger.info("Monitor pool closed.");
        }
    }

    /**
     * The type Monitor thread.
     */
    class MonitorThread implements Runnable {

        /**
         * The constant tagger.
         */
        public static final String tagger = "MonitorThread";

        private final BlockingQueue<T> blockingQueue;

        /**
         * Instantiates a new Monitor thread.
         *
         * @param blockingQueue the blocking queue
         */
        public MonitorThread(BlockingQueue<T> blockingQueue) {

            this.blockingQueue = blockingQueue;
        }

        @Override
        public void run() {

            // queue usage size
            int usageSize = blockingQueue.size();

            // queue free size
            int freeSize = blockingQueue.remainingCapacity();

            // queue usage rate (%)
            double usageRate = (usageSize * 1.0) / ((usageSize + freeSize) * 1.0) * 100;

            // usageRate > monitorPercentage
            if (usageRate > monitorPercentage)

                logger.warn("BlockingQueue usage rate: {}%. usage size: {}. free size: {}.", usageRate, usageSize, freeSize);
        }

    }

}
