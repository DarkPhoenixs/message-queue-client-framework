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

import org.darkphoenixs.benchmark.demo.MQConsumerDemo;
import org.darkphoenixs.benchmark.demo.MQMessageDecoderDemo;
import org.darkphoenixs.benchmark.utils.Counter;
import org.darkphoenixs.kafka.core.KafkaDestination;
import org.darkphoenixs.kafka.pool.KafkaMessageNewReceiverPool;
import org.darkphoenixs.mq.listener.MQMessageListenerAdapter;

import java.util.Properties;

public class MQConsumerWithFrameworkMain {

    public static void main(String args[]) throws Exception {

        final int theads = args.length > 0 ? Integer.valueOf(args[0]) : 10;
        final int counts = args.length > 1 ? Integer.valueOf(args[1]) : 1000000;

        Counter counter = new Counter(counts);
        MQMessageDecoderDemo decoderDemo = new MQMessageDecoderDemo();
        MQConsumerDemo consumerDemo = new MQConsumerDemo();
        consumerDemo.setCounter(counter);
        MQMessageListenerAdapter listenerAdapter = new MQMessageListenerAdapter();
        listenerAdapter.setMessageDecoder(decoderDemo);
        listenerAdapter.setConsumerAdapter(consumerDemo);
        listenerAdapter.setType("KAFKA");
        KafkaMessageNewReceiverPool receiverPool = new KafkaMessageNewReceiverPool();
        receiverPool.setDestination(new KafkaDestination("TEST1"));
        receiverPool.setMessageAdapter(listenerAdapter.getKafkaMessageAdapter());
        receiverPool.setProps(getProperties());
        receiverPool.setPoolSize(theads);
        receiverPool.init();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> receiverPool.destroy()));
        Thread.sleep(Long.MAX_VALUE);
    }

    public static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("client.id", "client_testWithFramework");
        properties.setProperty("group.id", "group_testWithFramework");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return properties;
    }

}
