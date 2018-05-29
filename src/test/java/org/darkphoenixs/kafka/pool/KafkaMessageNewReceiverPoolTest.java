/*
 * Copyright (c) 2016. Dark Phoenixs (Open-Source Organization).
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

package org.darkphoenixs.kafka.pool;

import kafka.admin.TopicCommand;
import kafka.message.MessageAndMetadata;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.darkphoenixs.kafka.codec.KafkaMessageDecoder;
import org.darkphoenixs.kafka.codec.KafkaMessageDecoderImpl;
import org.darkphoenixs.kafka.codec.KafkaMessageEncoderImpl;
import org.darkphoenixs.kafka.consumer.MessageConsumer;
import org.darkphoenixs.kafka.core.KafkaDestination;
import org.darkphoenixs.kafka.core.KafkaMessageAdapter;
import org.darkphoenixs.kafka.core.KafkaMessageTemplate;
import org.darkphoenixs.kafka.listener.KafkaMessageConsumerListener;
import org.darkphoenixs.kafka.producer.MessageProducer;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.darkphoenixs.mq.util.MQ_BATCH;
import org.darkphoenixs.mq.util.MQ_MODEL;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import scala.Option;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaMessageNewReceiverPoolTest {

    private int brokerId = 0;
    private String topic = "QUEUE.TEST";
    private String zkConnect;
    private EmbeddedZookeeper zkServer;
    private ZkClient zkClient;
    private KafkaServer kafkaServer;
    private int port = 9999;
    private Properties kafkaProps;

    @Before
    public void setUp() throws Exception {

        zkServer = new EmbeddedZookeeper();
        zkConnect = String.format("localhost:%d", zkServer.port());
        ZkUtils zkUtils = ZkUtils.apply(zkConnect, 30000, 30000,
                JaasUtils.isZkSecurityEnabled());
        zkClient = zkUtils.zkClient();

        final Option<File> noFile = scala.Option.apply(null);
        final Option<SecurityProtocol> noInterBrokerSecurityProtocol = scala.Option.apply(null);
        final Option<Properties> noPropertiesOption = scala.Option.apply(null);
        final Option<String> noStringOption = scala.Option.apply(null);

        kafkaProps = TestUtils.createBrokerConfig(brokerId, zkConnect, false,
                false, port, noInterBrokerSecurityProtocol, noFile, noPropertiesOption, true,
                false, TestUtils.RandomPort(), false, TestUtils.RandomPort(),
                false, TestUtils.RandomPort(), noStringOption, TestUtils.RandomPort());

        kafkaProps.setProperty("auto.create.topics.enable", "true");
        kafkaProps.setProperty("num.partitions", "1");
        // We *must* override this to use the port we allocated (Kafka currently
        // allocates one port
        // that it always uses for ZK
        kafkaProps.setProperty("zookeeper.connect", this.zkConnect);

        KafkaConfig config = new KafkaConfig(kafkaProps);
        Time mock = new SystemTime();
        kafkaServer = TestUtils.createServer(config, mock);

        // create topic
        TopicCommand.TopicCommandOptions options = new TopicCommand.TopicCommandOptions(
                new String[]{"--create", "--topic", topic,
                        "--replication-factor", "1", "--partitions", "2"});

        TopicCommand.createTopic(zkUtils, options);

        List<KafkaServer> servers = new ArrayList<KafkaServer>();
        servers.add(kafkaServer);
        TestUtils.waitUntilMetadataIsPropagated(
                scala.collection.JavaConversions.asScalaBuffer(servers), topic,
                0, 5000);
    }

    @After
    public void tearDown() throws Exception {
        try {
            kafkaServer.shutdown();
            zkClient.close();
            zkServer.shutdown();
        } catch (Exception e) {
        }
    }

    @Test
    public void test0() throws Exception {

        KafkaMessageNewReceiverPool<byte[], byte[]> pool = new KafkaMessageNewReceiverPool<byte[], byte[]>();

        pool.offsetCommitCallback.onComplete(null, null);

        pool.offsetCommitCallback.onComplete(null, new RuntimeException("test"));

        pool.returnReceiver(null);

        try {
            pool.returnReceiver(pool.getReceiver());
        } catch (Exception e) {
        }

        pool.destroy();

        Assert.assertFalse(pool.isRunning());

        Assert.assertNull(pool.getConfig());

        Assert.assertEquals(pool.getPoolSize(), 0);

        Assert.assertNotNull(pool.getProps());

        Assert.assertNull(pool.getMessageAdapter());

        Assert.assertEquals(pool.getModel(), "MODEL_1");

        pool.setModel("MODEL_1");

        Assert.assertEquals(pool.getBatch(), "NON_BATCH");

        pool.setBatch("BATCH");

        Assert.assertEquals(pool.getCommit(), "AUTO_COMMIT");

        pool.setCommit("AUTO_COMMIT");
        pool.setCommit("SYNC_COMMIT");
        pool.setCommit("ASYNC_COMMIT");

        Assert.assertEquals(pool.getRetryCount(), 3);

        pool.setRetryCount(1);

        Assert.assertEquals(pool.getHandleMultiple(), 2);

        pool.setHandleMultiple(1);

        Assert.assertEquals(pool.getQueueSize(), 100000);

        pool.setQueueSize(10000);

        Assert.assertEquals(pool.getThreadSleep(), 0);

        pool.setPollTimeout(2000);

        Assert.assertEquals(pool.getPollTimeout(), 2000);

        pool.setThreadSleep(1);

        pool.setPoolSize(10);

        pool.setProps(new Properties());

        pool.setConfig(new DefaultResourceLoader()
                .getResource("kafka/newconsumer.properties"));

        pool.setConfig(new DefaultResourceLoader()
                .getResource("kafka/newconsumer1.properties"));

        pool.getProps().setProperty("bootstrap.servers", "localhost:" + port);

        pool.getGroupId();

        pool.getClientId();

        KafkaMessageAdapter<Integer, MessageBeanImpl> adapter = new KafkaMessageAdapter<Integer, MessageBeanImpl>();

        adapter.setDecoder(new KafkaMessageDecoderImpl());

        KafkaMessageConsumerListener<Integer, MessageBeanImpl> listener = new KafkaMessageConsumerListener<Integer, MessageBeanImpl>();

        listener.setConsumer(new MessageConsumer<Integer, MessageBeanImpl>());

        adapter.setMessageListener(listener);

        adapter.setModel(MQ_MODEL.MODEL_1.name());

        adapter.setBatch(MQ_BATCH.NON_BATCH.name());

        pool.setMessageAdapter(adapter);

        pool.getDestination();
        pool.setDestination(new KafkaDestination(topic));

        pool.init();

        Assert.assertTrue(pool.isRunning());

        pool.destroy();

        Assert.assertFalse(pool.isRunning());

        pool.setPoolSize(1);

        pool.setRetryCount(0);

        pool.setBatch("BATCH");

        pool.init();

        pool.destroy();

        pool.setPoolSize(0);

        pool.setRetryCount(1);

        pool.init();

        pool.destroy();

        pool.setModel("MODEL_2");

        pool.init();

        pool.destroy();
    }


    @Test
    public void test_() throws Exception {

        KafkaMessageNewReceiverPool<byte[], byte[]> recePool = new KafkaMessageNewReceiverPool<byte[], byte[]>();

        recePool.setConfig(new DefaultResourceLoader()
                .getResource("kafka/newconsumer.properties"));

        recePool.getProps().setProperty("bootstrap.servers", "localhost:" + port);

        recePool.setPoolSize(4);

        recePool.setCommit("SYNC_COMMIT");

        recePool.setMessageAdapter(getAdapter());

        recePool.init();

        Thread.sleep(2000);

        KafkaMessageNewSenderPool<byte[], byte[]> sendPool = new KafkaMessageNewSenderPool<byte[], byte[]>();

        sendPool.setConfig(new DefaultResourceLoader()
                .getResource("kafka/newproducer.properties"));

        sendPool.getProps().setProperty("bootstrap.servers", "localhost:" + port);

        sendPool.init();

        KafkaMessageTemplate<Integer, MessageBeanImpl> kafkaMessageTemplate = new KafkaMessageTemplate<Integer, MessageBeanImpl>();

        kafkaMessageTemplate.setMessageSenderPool(sendPool);

        kafkaMessageTemplate.setEncoder(new KafkaMessageEncoderImpl());

        MessageProducer<Integer, MessageBeanImpl> messageProducer = new MessageProducer<Integer, MessageBeanImpl>();

        messageProducer.setMessageTemplate(kafkaMessageTemplate);

        messageProducer.setDestination(new KafkaDestination(topic));

        for (int i = 0; i < 10; i++) {

            messageProducer.sendWithKey(i, getMessage());
        }

        Thread.sleep(2000);

        recePool.setCommit("ASYNC_COMMIT");

        for (int i = 0; i < 10; i++) {

            messageProducer.sendWithKey(i, getMessage());
        }

        sendPool.destroy();

        recePool.destroy();

        Thread.sleep(2000);

    }

    @Test
    public void test1() throws Exception {

        KafkaMessageNewReceiverPool<byte[], byte[]> recePool = new KafkaMessageNewReceiverPool<byte[], byte[]>();

        recePool.setConfig(new DefaultResourceLoader()
                .getResource("kafka/newconsumer.properties"));

        recePool.getProps().setProperty("bootstrap.servers", "localhost:" + port);

        recePool.setPoolSize(4);

        recePool.setMessageAdapter(getAdapter());

        recePool.init();

        Thread.sleep(2000);

        KafkaMessageNewSenderPool<byte[], byte[]> sendPool = new KafkaMessageNewSenderPool<byte[], byte[]>();

        sendPool.setConfig(new DefaultResourceLoader()
                .getResource("kafka/newproducer.properties"));

        sendPool.getProps().setProperty("bootstrap.servers", "localhost:" + port);

        sendPool.init();

        KafkaMessageTemplate<Integer, MessageBeanImpl> kafkaMessageTemplate = new KafkaMessageTemplate<Integer, MessageBeanImpl>();

        kafkaMessageTemplate.setMessageSenderPool(sendPool);

        kafkaMessageTemplate.setEncoder(new KafkaMessageEncoderImpl());

        MessageProducer<Integer, MessageBeanImpl> messageProducer = new MessageProducer<Integer, MessageBeanImpl>();

        messageProducer.setMessageTemplate(kafkaMessageTemplate);

        messageProducer.setDestination(new KafkaDestination(topic));

        for (int i = 0; i < 10; i++) {

            messageProducer.sendWithKey(i, getMessage());
        }

        Thread.sleep(2000);

        recePool.setBatch("BATCH");

        recePool.setCommit("SYNC_COMMIT");

        for (int i = 0; i < 10; i++) {

            messageProducer.sendWithKey(i, getMessage());
        }

        Thread.sleep(2000);

        sendPool.destroy();

        recePool.destroy();
    }

    @Test
    public void test4() throws Exception {

        KafkaMessageNewReceiverPool<byte[], byte[]> recePool = new KafkaMessageNewReceiverPool<byte[], byte[]>();

        recePool.setConfig(new DefaultResourceLoader()
                .getResource("kafka/newconsumer.properties"));

        recePool.getProps().setProperty("bootstrap.servers", "localhost:" + port);

        recePool.setModel("MODEL_2");

        recePool.setPoolSize(4);

        recePool.setRetryCount(1);

        recePool.setMessageAdapter(getAdapterWishErr());

        recePool.init();

        Thread.sleep(2000);

        KafkaMessageNewSenderPool<byte[], byte[]> sendPool = new KafkaMessageNewSenderPool<byte[], byte[]>();

        sendPool.setConfig(new DefaultResourceLoader()
                .getResource("kafka/newproducer.properties"));

        sendPool.getProps().setProperty("bootstrap.servers", "localhost:" + port);

        sendPool.init();

        KafkaMessageTemplate<Integer, MessageBeanImpl> kafkaMessageTemplate = new KafkaMessageTemplate<Integer, MessageBeanImpl>();

        kafkaMessageTemplate.setMessageSenderPool(sendPool);

        kafkaMessageTemplate.setEncoder(new KafkaMessageEncoderImpl());

        MessageProducer<Integer, MessageBeanImpl> messageProducer = new MessageProducer<Integer, MessageBeanImpl>();

        messageProducer.setMessageTemplate(kafkaMessageTemplate);

        messageProducer.setDestination(new KafkaDestination(topic));

        for (int i = 0; i < 10; i++) {

            messageProducer.sendWithKey(i, getMessage());
        }

        Thread.sleep(2000);

        recePool.setBatch("_BATCH");

        recePool.setCommit("SYNC_COMMIT");

        for (int i = 0; i < 10; i++) {

            messageProducer.sendWithKey(i, getMessage());
        }

        Thread.sleep(2000);

        sendPool.destroy();

        recePool.destroy();
    }

    private KafkaMessageAdapter<Integer, MessageBeanImpl> getAdapter() {

        KafkaMessageDecoderImpl messageDecoder = new KafkaMessageDecoderImpl();

        KafkaDestination kafkaDestination = new KafkaDestination(topic);

        MessageConsumer<Integer, MessageBeanImpl> MessageConsumer = new MessageConsumer<Integer, MessageBeanImpl>() {

            @Override
            protected void doReceive(MessageBeanImpl message) throws MQException {
                System.out.println(message);
            }

            @Override
            protected void doReceive(Integer key, MessageBeanImpl val) throws MQException {
                System.out.println(key + ":" + val);
            }

            @Override
            protected void doReceive(Map<Integer, MessageBeanImpl> messages) throws MQException {
                System.out.println(messages);
            }
        };

        KafkaMessageConsumerListener<Integer, MessageBeanImpl> messageConsumerListener = new KafkaMessageConsumerListener<Integer, MessageBeanImpl>();

        messageConsumerListener.setConsumer(MessageConsumer);

        KafkaMessageAdapter<Integer, MessageBeanImpl> messageAdapter = new KafkaMessageAdapter<Integer, MessageBeanImpl>();

        messageAdapter.setDecoder(messageDecoder);

        messageAdapter.setDestination(kafkaDestination);

        messageAdapter.setMessageListener(messageConsumerListener);

        return messageAdapter;
    }

    private KafkaMessageAdapter<Integer, MessageBeanImpl> getAdapterWishErr() {

        KafkaMessageDecoder<Integer, MessageBeanImpl> messageDecoder = new KafkaMessageDecoder<Integer, MessageBeanImpl>() {

            @Override
            public MessageBeanImpl decode(byte[] bytes) throws MQException {

                throw new MQException("Test");
            }

            @Override
            public List<MessageBeanImpl> batchDecode(List<byte[]> bytes)
                    throws MQException {
                throw new MQException("Test");
            }

            @Override
            public Integer decodeKey(byte[] bytes) throws MQException {
                throw new MQException("Test");
            }

            @Override
            public MessageBeanImpl decodeVal(byte[] bytes) throws MQException {
                throw new MQException("Test");
            }

            @Override
            public Map<Integer, MessageBeanImpl> batchDecode(
                    Map<byte[], byte[]> bytes) throws MQException {
                throw new MQException("Test");
            }
        };

        KafkaDestination kafkaDestination = new KafkaDestination(topic);

        MessageConsumer<Integer, MessageBeanImpl> MessageConsumer = new MessageConsumer<Integer, MessageBeanImpl>();

        KafkaMessageConsumerListener<Integer, MessageBeanImpl> messageConsumerListener = new KafkaMessageConsumerListener<Integer, MessageBeanImpl>();

        messageConsumerListener.setConsumer(MessageConsumer);

        KafkaMessageAdapter<Integer, MessageBeanImpl> messageAdapter = new KafkaMessageAdapter<Integer, MessageBeanImpl>() {
            @Override
            public void messageAdapter(MessageAndMetadata<?, ?> messageAndMetadata) throws MQException {
                throw new MQException("Test");
            }

            @Override
            public void messageAdapter(ConsumerRecord<?, ?> consumerRecord) throws MQException {
                throw new MQException("Test");
            }

            @Override
            public void messageAdapter(ConsumerRecords<?, ?> consumerRecords) throws MQException {
                throw new MQException("Test");
            }
        };

        messageAdapter.setDecoder(messageDecoder);

        messageAdapter.setDestination(kafkaDestination);

        messageAdapter.setMessageListener(messageConsumerListener);

        return messageAdapter;
    }

    private MessageBeanImpl getMessage() {

        MessageBeanImpl messageBean = new MessageBeanImpl();

        long date = System.currentTimeMillis();
        messageBean.setMessageNo("MessageNo");
        messageBean.setMessageType("MessageType");
        messageBean.setMessageAckNo("MessageAckNo");
        messageBean.setMessageDate(date);
        messageBean.setMessageContent("MessageContent".getBytes());

        return messageBean;
    }

}