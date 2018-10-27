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

package org.darkphoenixs.benchmark.natives.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.darkphoenixs.benchmark.utils.Counter;
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

    @Override
    public void run(ApplicationArguments args) throws Exception {

        KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<byte[], byte[]>(getProperties());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaProducer.close()));

        for (int i = 0; i < theads; i++) {

            Thread thread = new Thread(() -> {

                Counter counter = new Counter(counts);

                counter.setBegin(System.currentTimeMillis());

                for (int l = 0; l < counter.getCount() / theads; l++) {

                    kafkaProducer.send(new ProducerRecord<>(topic, String.valueOf(l).getBytes(), message.getBytes()));
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
