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

package org.darkphoenixs.benchmark.natives.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.darkphoenixs.benchmark.utils.Counter;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerRunner implements Runnable {

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer consumer;
    private final Counter counter;
    private final String topic;

    public KafkaConsumerRunner(Properties props, Counter counter, String topic) {
        this.counter = counter;
        this.consumer = new KafkaConsumer(props);
        this.topic = topic;
    }

    public void run() {
        try {
            consumer.subscribe(Arrays.asList(topic));
            while (!closed.get()) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(10000);
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    receive(new String(record.value()));
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
        }
    }

    public void receive(String message) {
        // Do something...
        if (counter.add() % counter.getCount() == 1) {
            counter.setBegin(System.currentTimeMillis());
        }
        if (counter.get() % 10000 == 0) {
            System.err.println("receive count 10000.");
        }
        if (counter.get() % counter.getCount() == 0) {
            counter.setEnd(System.currentTimeMillis());
            System.err.println("receive time " + counter.getTime()
                    + " s. average receive " + counter.getAverage() + "/s.");
        }
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

}
