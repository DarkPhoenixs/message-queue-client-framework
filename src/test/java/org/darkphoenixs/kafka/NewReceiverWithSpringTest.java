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

package org.darkphoenixs.kafka;

import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.darkphoenixs.kafka.codec.KafkaMessageEncoderImpl;
import org.darkphoenixs.kafka.core.KafkaDestination;
import org.darkphoenixs.kafka.core.KafkaMessageTemplate;
import org.darkphoenixs.kafka.pool.KafkaMessageNewReceiverPool;
import org.darkphoenixs.kafka.pool.KafkaMessageNewSenderPool;
import org.darkphoenixs.kafka.producer.MessageProducer;
import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import scala.Option;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"classpath*:/kafka/applicationContext-newconsumer4test.xml"})
public class NewReceiverWithSpringTest {

    private int brokerId = 0;
    private String topic = "QUEUE.TEST";
    private String zkConnect;
    private EmbeddedZookeeper zkServer;
    private ZkClient zkClient;
    private KafkaServer kafkaServer;
    private int port = 9999;
    private Properties kafkaProps;
    @Autowired
    private KafkaMessageNewReceiverPool<byte[], byte[]> recePool;

    @Before
    public void before() {

        zkServer = new EmbeddedZookeeper();
        zkConnect = String.format("localhost:%d", zkServer.port());
        ZkUtils zkUtils = ZkUtils.apply(zkConnect, 30000, 30000,
                JaasUtils.isZkSecurityEnabled());
        zkClient = zkUtils.zkClient();

        Time mock = new SystemTime();
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
        kafkaProps.setProperty("host.name", "localhost");
        kafkaProps.setProperty("port", port + "");

        KafkaConfig config = new KafkaConfig(kafkaProps);
        kafkaServer = TestUtils.createServer(config, mock);

        // create topic
        TopicCommand.TopicCommandOptions options = new TopicCommand.TopicCommandOptions(
                new String[]{"--create", "--topic", topic,
                        "--replication-factor", "1", "--partitions", "1"});

        TopicCommand.createTopic(zkUtils, options);

        List<KafkaServer> servers = new ArrayList<KafkaServer>();
        servers.add(kafkaServer);
        TestUtils.waitUntilMetadataIsPropagated(
                scala.collection.JavaConversions.asScalaBuffer(servers), topic,
                0, 5000);
    }

    @After
    public void after() {
        try {
            kafkaServer.shutdown();
            zkClient.close();
            zkServer.shutdown();
        } catch (Exception e) {
        }
    }

    @Test
    public void test() throws Exception {

        recePool.init();

        KafkaMessageNewSenderPool<byte[], byte[]> sendPool = new KafkaMessageNewSenderPool<byte[], byte[]>();

        sendPool.setConfig(new DefaultResourceLoader()
                .getResource("kafka/newproducer.properties"));

        sendPool.init();

        Thread.sleep(2000);

        KafkaMessageTemplate<Integer, MessageBeanImpl> kafkaMessageTemplate = new KafkaMessageTemplate<Integer, MessageBeanImpl>();

        kafkaMessageTemplate.setMessageSenderPool(sendPool);

        kafkaMessageTemplate.setEncoder(new KafkaMessageEncoderImpl());

        MessageProducer<Integer, MessageBeanImpl> messageProducer = new MessageProducer<Integer, MessageBeanImpl>();

        messageProducer.setMessageTemplate(kafkaMessageTemplate);

        messageProducer.setDestination(new KafkaDestination(topic));

        for (int i = 0; i < 10; i++)

            messageProducer.sendWithKey(i, getMessage());

        sendPool.destroy();


        recePool.destroy();
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
