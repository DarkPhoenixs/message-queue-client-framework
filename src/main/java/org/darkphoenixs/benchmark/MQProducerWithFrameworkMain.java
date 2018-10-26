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

import org.darkphoenixs.benchmark.demo.MQMessageEncoderDemo;
import org.darkphoenixs.benchmark.demo.MQProducerDemo;
import org.darkphoenixs.benchmark.utils.Counter;
import org.darkphoenixs.kafka.pool.KafkaMessageNewSenderPool;
import org.darkphoenixs.mq.exception.MQException;

import java.util.Properties;

public class MQProducerWithFrameworkMain {

    public static final String message = "1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM";

    public static void main(String args[]) throws Exception {

        final int theads = args.length > 0 ? Integer.valueOf(args[0]) : 10;
        final int counts = args.length > 1 ? Integer.valueOf(args[1]) : 1000000;

        KafkaMessageNewSenderPool senderPool = new KafkaMessageNewSenderPool();
        senderPool.setProps(getProperties());
        senderPool.init();
        MQMessageEncoderDemo encoderDemo = new MQMessageEncoderDemo();
        MQProducerDemo producerDemo = new MQProducerDemo();
        producerDemo.setMessageEncoder(encoderDemo);
        producerDemo.setKafkaMessageSenderPool(senderPool);
        producerDemo.setTopic("TEST");
        producerDemo.setType("KAFKA");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> senderPool.destroy()));

        Thread.sleep(5000);

        for (int i = 0; i < theads; i++) {

            Thread thread = new Thread(() -> {

                Counter counter = new Counter(counts);

                counter.setBegin(System.currentTimeMillis());

                for (int l = 0; l < counter.getCount() / theads; l++) {

                    try {
                        producerDemo.sendWithKey(String.valueOf(l), message);
                    } catch (MQException e) {
                        e.printStackTrace();
                    }
                }
                counter.setEnd(System.currentTimeMillis());

                System.err.println("send time " + counter.getTime() + " s. average send " + counter.getAverage() + "/s.");
            });

            thread.start();
        }

        Thread.sleep(Long.MAX_VALUE);
    }

    public static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("client.id", "client_testWithFramework");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return properties;
    }
}
