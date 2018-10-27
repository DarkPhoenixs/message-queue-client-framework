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

package org.darkphoenixs.org.darkphoenixs.benchmark.client.consumer;

import org.darkphoenixs.benchmark.utils.Counter;
import org.darkphoenixs.kafka.core.KafkaDestination;
import org.darkphoenixs.kafka.pool.KafkaMessageNewReceiverPool;
import org.darkphoenixs.mq.listener.MQMessageListenerAdapter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class ConsumerApplicationRunner implements ApplicationRunner {

    @Value("${theads}")
    private int theads;

    @Value("${counts}")
    private int counts;

    @Value("${bootstrap.servers}")
    private String servers;

    @Value("${group.id}")
    private String group;

    @Value("${receive.topic}")
    private String topic;

    @Value("${mq.type}")
    private String type;

    @Override
    public void run(ApplicationArguments args) throws Exception {

        Counter counter = new Counter(counts);
        ConsumerDecoderDemo decoderDemo = new ConsumerDecoderDemo();
        ConsumerDemo consumerDemo = new ConsumerDemo();
        consumerDemo.setCounter(counter);
        MQMessageListenerAdapter listenerAdapter = new MQMessageListenerAdapter();
        listenerAdapter.setMessageDecoder(decoderDemo);
        listenerAdapter.setConsumerAdapter(consumerDemo);
        listenerAdapter.setType(type);
        KafkaMessageNewReceiverPool receiverPool = new KafkaMessageNewReceiverPool();
        receiverPool.setDestination(new KafkaDestination(topic));
        receiverPool.setMessageAdapter(listenerAdapter.getKafkaMessageAdapter());
        receiverPool.setProps(getProperties());
        receiverPool.setPoolSize(theads);
        receiverPool.init();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> receiverPool.destroy()));

    }

    public Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", servers);
        properties.setProperty("group.id", group);
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return properties;
    }
}
