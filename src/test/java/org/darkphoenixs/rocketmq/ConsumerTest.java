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


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.darkphoenixs.rocketmq.codec.RocketmqMessageDecoderDemo;
import org.darkphoenixs.rocketmq.consumer.MessageConsumer;
import org.darkphoenixs.rocketmq.listener.RocketmqMessageConsumerListener;
import org.junit.Test;

public class ConsumerTest {

    @Test
    public void test() throws Exception {

        RocketmqMessageConsumerListener messageConsumerListener = new RocketmqMessageConsumerListener();

        messageConsumerListener.setModel("MODEL_1");
        messageConsumerListener.setBatch("NON_BATCH");
        messageConsumerListener.setMessageDecoder(new RocketmqMessageDecoderDemo());
        messageConsumerListener.setConsumer(new MessageConsumer());

        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer();

        defaultMQPushConsumer.setNamesrvAddr("localhost:9876");
        defaultMQPushConsumer.setConsumerGroup("contest");
        defaultMQPushConsumer.subscribe("QUEUE_TEST", "*");
        defaultMQPushConsumer.setMessageListener(messageConsumerListener.getMessageListener());

        defaultMQPushConsumer.start();
    }
}
