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

package org.darkphoenixs.kafka.core;

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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Option;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaMessageNewReceiverTest {

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
    public void tearDown() throws Exception {
        try {
            kafkaServer.shutdown();
            zkClient.close();
            zkServer.shutdown();
        } catch (Exception e) {
        }
    }

    @Test
    public void test() throws Exception {

        Properties sendProperties = new Properties();
        sendProperties.setProperty("bootstrap.servers", "localhost:" + port);
        sendProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        sendProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        @SuppressWarnings("unchecked")
        KafkaMessageNewSender<byte[], byte[]> sender = new KafkaMessageNewSender(sendProperties);

        for (int i = 0; i < 10; i++) {

            sender.sendWithKey(topic, ("key" + i).getBytes(), ("value" + i).getBytes());
        }

        sender.shutDown();

        Properties receProperties = new Properties();
        receProperties.setProperty("bootstrap.servers", "localhost:" + port);
        receProperties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        receProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        receProperties.put("group.id", "KafkaMessageNewReceiverTest");

        KafkaMessageReceiver<byte[], byte[]> receiver = new KafkaMessageNewReceiver<byte[], byte[]>(receProperties);

        Assert.assertEquals(receiver.getPartitionCount(topic), 1);

        Assert.assertEquals(receiver.getEarliestOffset(topic, 0), 0);

        Assert.assertEquals(receiver.getLatestOffset(topic, 0), 10);

        List<byte[]> vals1 = receiver.receive(topic, 0, 0, 2);

        Assert.assertEquals(vals1.size(), 2);

        receiver.receive(topic, 0, 2, 5);

        try {
            receiver.receive(topic, 0, 2, 0);
        } catch (Exception e) {

        }
        receiver.receive(topic, 0, -1, 10);

        receiver.receive(topic, 0, -1, 11);

        Map<byte[], byte[]> maps1 = receiver.receiveWithKey(topic, 0, 1, 2);

        Assert.assertEquals(maps1.size(), 2);

        receiver.receiveWithKey(topic, 0, 3, 5);

        try {
            receiver.receiveWithKey(topic, 0, 3, 0);
        } catch (Exception e) {

        }

        receiver.receiveWithKey(topic, 0, -1, 10);

        receiver.receiveWithKey(topic, 0, -1, 11);

        receiver.shutDown();

        try {
            receiver.shutDown();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}