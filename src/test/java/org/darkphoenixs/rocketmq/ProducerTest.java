/*
 * Copyright (c) 2017. Dark Phoenixs (Open-Source Organization).
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

package org.darkphoenixs.rocketmq;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.darkphoenixs.rocketmq.codec.RocketmqMessageEncoderDemo;
import org.darkphoenixs.rocketmq.producer.MessageProducer;
import org.junit.Test;

public class ProducerTest {

    @Test
    public void test() throws Exception {

        DefaultMQProducer defaultMQProducer = new DefaultMQProducer();
        defaultMQProducer.setNamesrvAddr("localhost:9876");
        defaultMQProducer.setProducerGroup("protest");
        defaultMQProducer.start();

        MessageProducer messageProducer = new MessageProducer();
        messageProducer.setDefaultMQProducer(defaultMQProducer);
        messageProducer.setMessageEncoder(new RocketmqMessageEncoderDemo());
        messageProducer.setTopic("QUEUE_TEST");

//        for (int i = 0; i < 100; i++)
//
//            messageProducer.sendWithKey(String.valueOf(i), "test"+i);

        defaultMQProducer.shutdown();
    }
}
