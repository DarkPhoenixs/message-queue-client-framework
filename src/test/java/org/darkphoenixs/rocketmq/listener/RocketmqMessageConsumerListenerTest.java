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

package org.darkphoenixs.rocketmq.listener;

import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;
import org.darkphoenixs.rocketmq.codec.RocketmqMessageDecoderTest;
import org.darkphoenixs.rocketmq.consumer.MessageConsumer;
import org.darkphoenixs.rocketmq.listener.RocketmqMessageConsumerListener.MODEL;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import static org.darkphoenixs.rocketmq.listener.RocketmqMessageConsumerListener.BATCH;

public class RocketmqMessageConsumerListenerTest {

    @Test
    public void getMessageListener() throws Exception {

        RocketmqMessageConsumerListener<String> listener = new RocketmqMessageConsumerListener<String>();
        Assert.assertNotNull(listener.getBatch());
        Assert.assertNotNull(listener.getModel());
        Assert.assertNull(listener.getConsumer());
        Assert.assertNull(listener.getMessageDecoder());

        Assert.assertTrue(listener.getMessageListener() instanceof MessageListenerOrderly);

        listener.setBatch(BATCH.NON_BATCH.name());
        listener.setModel(MODEL.MODEL_2.name());
        listener.setConsumer(new MessageConsumer<String>());
        listener.setMessageDecoder(new RocketmqMessageDecoderTest());

        Assert.assertTrue(listener.getMessageListener() instanceof MessageListenerConcurrently);

    }

    @Test
    public void onMessage1() throws Exception {

        RocketmqMessageConsumerListener<String> listener = new RocketmqMessageConsumerListener<String>();

        try {
            listener.onMessage("test");
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        listener.setConsumer(new MessageConsumer<String>());

        listener.onMessage("test");
    }

    @Test
    public void onMessage2() throws Exception {

        RocketmqMessageConsumerListener<String> listener = new RocketmqMessageConsumerListener<String>();

        try {
            listener.onMessage(Collections.singletonList("test"));
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        listener.setConsumer(new MessageConsumer<String>());

        listener.onMessage(Collections.singletonList("test"));
    }


    @Test
    public void consume() throws Exception {

        RocketmqMessageConsumerListener<String> listener = new RocketmqMessageConsumerListener<String>();

        MessageExt messageExt = new MessageExt();
        messageExt.setKeys("key");
        messageExt.setBody("test".getBytes());

        try {
            listener.messageListenerOrderly.consumeMessage(Collections.singletonList(new MessageExt()), null);
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }
        listener.setConsumer(new MessageConsumer<String>());
        listener.setMessageDecoder(new RocketmqMessageDecoderTest());
        listener.messageListenerOrderly.consumeMessage(Collections.singletonList(messageExt), null);

        listener.setBatch(BATCH.BATCH.name());

        try {
            listener.messageListenerConcurrently.consumeMessage(Collections.singletonList(new MessageExt()), null);
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }
        listener.setConsumer(new MessageConsumer<String>());
        listener.setMessageDecoder(new RocketmqMessageDecoderTest());
        listener.messageListenerConcurrently.consumeMessage(Collections.singletonList(messageExt), null);
    }

}