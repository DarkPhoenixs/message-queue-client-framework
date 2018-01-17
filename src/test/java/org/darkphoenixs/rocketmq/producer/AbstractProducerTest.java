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

package org.darkphoenixs.rocketmq.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.rocketmq.codec.RocketmqMessageEncoderDemo;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class AbstractProducerTest {

    AbstractProducer<String> abstractProducer = new AbstractProducer<String>(){};

    @Test
    public void send() throws Exception {


        try {
            abstractProducer.send("test");
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        abstractProducer.setTopic("QUEUE_TEST");
        Assert.assertEquals(abstractProducer.getTopic(), "QUEUE_TEST");
        Assert.assertEquals(abstractProducer.getProducerKey(), "QUEUE_TEST");

        DefaultMQProducer defaultMQProducer = new DefaultMQProducer() {

            @Override
            public SendResult send(Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
                return new SendResult();
            }
        };

        abstractProducer.setDefaultMQProducer(defaultMQProducer);
        Assert.assertNotNull(abstractProducer.getDefaultMQProducer());

        try {
            abstractProducer.send("test");
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        abstractProducer.setMessageEncoder(new RocketmqMessageEncoderDemo());
        Assert.assertNotNull(abstractProducer.getMessageEncoder());

        abstractProducer.setProducerKey("test");
        Assert.assertEquals(abstractProducer.getProducerKey(), "test");


        abstractProducer.send("test");
    }

    @Test
    public void batchSend() throws Exception {

        try {
            abstractProducer.batchSend(Collections.singletonList("test"));
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        DefaultMQProducer defaultMQProducer = new DefaultMQProducer() {

            @Override
            public SendResult send(Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
                return new SendResult();
            }
        };

        abstractProducer.setDefaultMQProducer(defaultMQProducer);

        try {
            abstractProducer.batchSend(Collections.singletonList("test"));
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        abstractProducer.setMessageEncoder(new RocketmqMessageEncoderDemo());

        abstractProducer.batchSend(Collections.singletonList("test"));

    }

    @Test
    public void sendAsync() throws Exception {

        try {
            abstractProducer.sendAsync("test");
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        DefaultMQProducer defaultMQProducer = new DefaultMQProducer() {

            @Override
            public void send(Message msg, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
                sendCallback.onSuccess(new SendResult());
                sendCallback.onException(new RuntimeException("test"));
            }
        };

        abstractProducer.setDefaultMQProducer(defaultMQProducer);

        try {
            abstractProducer.sendAsync("test");
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        abstractProducer.setMessageEncoder(new RocketmqMessageEncoderDemo());

        abstractProducer.sendAsync("test");
    }

    @Test
    public void sendOneWay() throws Exception {

        try {
            abstractProducer.sendOneWay("test");
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        DefaultMQProducer defaultMQProducer = new DefaultMQProducer() {

            @Override
            public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
            }
        };

        abstractProducer.setDefaultMQProducer(defaultMQProducer);

        try {
            abstractProducer.sendOneWay("test");
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        abstractProducer.setMessageEncoder(new RocketmqMessageEncoderDemo());

        abstractProducer.sendOneWay("test");
    }

    @Test
    public void sendWithKey() throws Exception {

        try {
            abstractProducer.sendWithKey("key", "val");
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        DefaultMQProducer defaultMQProducer = new DefaultMQProducer() {

            @Override
            public SendResult send(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

                selector.select(Collections.singletonList(new MessageQueue()), msg, arg);

                return new SendResult();
            }
        };

        abstractProducer.setDefaultMQProducer(defaultMQProducer);

        try {
            abstractProducer.sendWithKey("key", "val");
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        abstractProducer.setMessageEncoder(new RocketmqMessageEncoderDemo());

        abstractProducer.sendWithKey("key", "val");
    }

    @Test
    public void sendWithTag() throws Exception {

        try {
            abstractProducer.sendWithTag("key", "tag", "val");
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        DefaultMQProducer defaultMQProducer = new DefaultMQProducer() {

            @Override
            public SendResult send(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

                selector.select(Collections.singletonList(new MessageQueue()), msg, arg);

                return new SendResult();
            }
        };

        abstractProducer.setDefaultMQProducer(defaultMQProducer);

        try {
            abstractProducer.sendWithTag("key", "tag", "val");
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        abstractProducer.setMessageEncoder(new RocketmqMessageEncoderDemo());

        abstractProducer.sendWithTag("key", "tag", "val");
    }

    @Test
    public void sendWithTx() throws Exception {

        try {
            abstractProducer.sendWithTx("test", null, "param");
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        TransactionMQProducer transactionMQProducer = new TransactionMQProducer() {

            @Override
            public TransactionSendResult sendMessageInTransaction(Message msg, LocalTransactionExecuter tranExecuter, Object arg) throws MQClientException {

                tranExecuter.executeLocalTransactionBranch(msg, arg);

                return new TransactionSendResult();
            }
        };

        abstractProducer.setTransactionMQProducer(transactionMQProducer);
        Assert.assertNotNull(abstractProducer.getTransactionMQProducer());

        try {
            abstractProducer.sendWithTx("test", null, "param");
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        abstractProducer.setMessageEncoder(new RocketmqMessageEncoderDemo());

        abstractProducer.sendWithTx("test", new LocalTransactionExecuter() {
            @Override
            public LocalTransactionState executeLocalTransactionBranch(Message message, Object o) {
                Assert.assertEquals("param", o);
                return LocalTransactionState.COMMIT_MESSAGE;
            }

        }, "param");
    }

}