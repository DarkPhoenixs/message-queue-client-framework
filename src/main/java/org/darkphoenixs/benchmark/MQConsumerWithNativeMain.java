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

package org.darkphoenixs.benchmark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.darkphoenixs.benchmark.demo.MQConsumerDemo;
import org.darkphoenixs.benchmark.utils.Counter;
import org.darkphoenixs.mq.exception.MQException;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class MQConsumerWithNativeMain {

    public static void main(String args[]) throws Exception {

        final int theads = args.length > 0 ? Integer.valueOf(args[0]) : 10;
        final int counts = args.length > 1 ? Integer.valueOf(args[1]) : 1000000;

        Counter counter = new Counter(counts);
        MQConsumerDemo consumerDemo = new MQConsumerDemo();
        consumerDemo.setCounter(counter);

        for (int i = 0; i < theads; i++) {
            KafkaConsumerRunner consumerRunner = new KafkaConsumerRunner(getProperties(), consumerDemo);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> consumerRunner.shutdown()));
            Thread thread = new Thread(consumerRunner);
            thread.start();
        }

        Thread.sleep(Long.MAX_VALUE);
    }

    public static class KafkaConsumerRunner implements Runnable {
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final KafkaConsumer consumer;
        private final MQConsumerDemo consumerDemo;

        public KafkaConsumerRunner(Properties props, MQConsumerDemo consumerDemo) {
            this.consumerDemo = consumerDemo;
            this.consumer = new KafkaConsumer(props);
        }

        public void run() {
            try {
                consumer.subscribe(Arrays.asList("TEST"));
                while (!closed.get()) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(10000);
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        consumerDemo.receive(new String(record.value()));
                    }
                }
            } catch (WakeupException e) {
                // Ignore exception if closing
                if (!closed.get()) throw e;
            } catch (MQException e) {
                e.printStackTrace();
            } finally {
                consumer.close();
            }
        }

        // Shutdown hook which can be called from a separate thread
        public void shutdown() {
            closed.set(true);
            consumer.wakeup();
        }
    }


    public static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("client.id", "client_testWithNative");
        properties.setProperty("group.id", "client_testWithNative");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return properties;
    }
}
