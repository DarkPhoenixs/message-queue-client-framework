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

package org.darkphoenixs.benchmark.client.producer;

import org.darkphoenixs.benchmark.utils.Counter;
import org.darkphoenixs.kafka.pool.KafkaMessageNewSenderPool;
import org.darkphoenixs.mq.exception.MQException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class ProducerApplicationRunner implements ApplicationRunner {

    public static final String message = "1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM";

    @Value("${theads}")
    private int theads;

    @Value("${counts}")
    private int counts;

    @Value("${bootstrap.servers}")
    private String servers;

    @Value("${client.id}")
    private String client;

    @Value("${send.topic}")
    private String topic;

    @Value("${mq.type}")
    private String type;

    @Override
    public void run(ApplicationArguments args) throws Exception {

        KafkaMessageNewSenderPool senderPool = new KafkaMessageNewSenderPool();
        senderPool.setProps(getProperties());
        senderPool.init();
        ProducerEncoderDemo encoderDemo = new ProducerEncoderDemo();
        ProducerDemo producerDemo = new ProducerDemo();
        producerDemo.setMessageEncoder(encoderDemo);
        producerDemo.setKafkaMessageSenderPool(senderPool);
        producerDemo.setTopic(topic);
        producerDemo.setType(type);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> senderPool.destroy()));

        for (int i = 0; i < theads; i++) {

            Thread thread = new Thread(() -> {

                Counter counter = new Counter(counts);

                counter.setBegin(System.currentTimeMillis());

                for (int l = 0; l < counter.getCount() / theads; l++) {

                    try {
                        producerDemo.send(message);
                    } catch (MQException e) {
                        e.printStackTrace();
                    }
                }
                counter.setEnd(System.currentTimeMillis());

                System.err.println("send time " + counter.getTime() + " s. average send " + counter.getAverage() + "/s.");
            });

            thread.start();
        }

    }

    public Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", servers);
        properties.setProperty("client.id", client);
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return properties;
    }
}
