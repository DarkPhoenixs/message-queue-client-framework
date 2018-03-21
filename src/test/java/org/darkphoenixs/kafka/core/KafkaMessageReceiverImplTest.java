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
import org.darkphoenixs.kafka.pool.KafkaMessageReceiverPool;
import org.darkphoenixs.kafka.pool.KafkaMessageSenderPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Option;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaMessageReceiverImplTest {

    private int brokerId = 0;
    private String topic = "QUEUE.TEST";
    private String zkConnect;
    private EmbeddedZookeeper zkServer;
    private ZkClient zkClient;
    private KafkaServer kafkaServer;
    private int port = 9999;
    private Properties kafkaProps;

    @Before
    public void before() {

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
        kafkaProps.setProperty("host.name", "localhost");
        kafkaProps.setProperty("port", port + "");

        Properties kafkaProps2 = TestUtils.createBrokerConfig(brokerId + 1,
                zkConnect, false, false, (port - 1),
                noInterBrokerSecurityProtocol, noFile, noPropertiesOption, true, false,
                TestUtils.RandomPort(), false, TestUtils.RandomPort(), false,
                TestUtils.RandomPort(), noStringOption, TestUtils.RandomPort());

        kafkaProps2.setProperty("auto.create.topics.enable", "true");
        kafkaProps2.setProperty("num.partitions", "1");
        // We *must* override this to use the port we allocated (Kafka currently
        // allocates one port
        // that it always uses for ZK
        kafkaProps2.setProperty("zookeeper.connect", this.zkConnect);
        kafkaProps2.setProperty("host.name", "localhost");
        kafkaProps2.setProperty("port", (port - 1) + "");

        KafkaConfig config = new KafkaConfig(kafkaProps);
        KafkaConfig config2 = new KafkaConfig(kafkaProps2);

        Time mock = new SystemTime();
        Time mock2 = new SystemTime();

        kafkaServer = TestUtils.createServer(config, mock);
        KafkaServer kafkaServer2 = TestUtils.createServer(config2, mock2);

        // create topic
        TopicCommand.TopicCommandOptions options = new TopicCommand.TopicCommandOptions(
                new String[]{"--create", "--topic", topic,
                        "--replication-factor", "2", "--partitions", "2"});

        TopicCommand.createTopic(zkUtils, options);

        List<KafkaServer> servers = new ArrayList<KafkaServer>();
        servers.add(kafkaServer);
        servers.add(kafkaServer2);
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

        KafkaMessageSenderPool<byte[], byte[]> sendPool = new KafkaMessageSenderPool<byte[], byte[]>();

        sendPool.setProps(TestUtils.getProducerConfig("localhost:" + port
                + ",localhost:" + (port - 1)));

        sendPool.init();

        Properties properties = TestUtils.getProducerConfig("localhost:" + port
                + ",localhost:" + (port - 1));

        KafkaMessageSenderImpl<byte[], byte[]> sender = new KafkaMessageSenderImpl<byte[], byte[]>(
                properties);

        Assert.assertNotNull(sender.getProducer());
        sender.setProducer(sender.getProducer());

        sender.send(topic, "test".getBytes());

        sender.sendWithKey(topic, "key".getBytes(), "value".getBytes());

        sender.sendWithKey(topic, "key".getBytes(), "value".getBytes());

        sender.shutDown();

        sendPool.destroy();

        Properties consumerProps = TestUtils.createConsumerProperties(
                zkConnect, "group_1", "consumer_1", 1000);

        KafkaMessageReceiverPool<byte[], byte[]> recePool = new KafkaMessageReceiverPool<byte[], byte[]>();

        recePool.setProps(consumerProps);
        recePool.setPoolSize(10);
        recePool.setClientId("test1");

        KafkaMessageReceiverImpl<byte[], byte[]> receiver = new KafkaMessageReceiverImpl<byte[], byte[]>(
                consumerProps, recePool);

        receiver.getEarliestOffset(topic, -1);

        receiver.getLatestOffset(topic, -1);

        receiver.getEarliestOffset(topic, 0);

        receiver.getLatestOffset(topic, 0);

        receiver.receive(topic, 0, 0, 1);

        receiver.receive(topic, 0, 0, 2);

        receiver.receive(topic, 0, 1, 2);

        receiver.receive(topic, 1, 0, 2);

        try {
            receiver.receive(topic, 1, 0, 0);
        } catch (Exception e) {

        }

        receiver.receiveWithKey(topic, 0, 1, 1);

        receiver.receiveWithKey(topic, 0, 1, 2);

        receiver.receiveWithKey(topic, 0, 2, 2);

        receiver.receiveWithKey(topic, 0, 1, 2);

        try {
            receiver.receiveWithKey(topic, 1, 1, 0);
        } catch (Exception e) {

        }

        receiver.shutDown();

        try {
            receiver.getEarliestOffset("test", 0);
        } catch (Exception e) {
        }

        try {
            receiver.getLatestOffset("test", 0);
        } catch (Exception e) {
        }

        try {
            receiver.getEarliestOffset(topic, 2);
        } catch (Exception e) {
        }

        try {
            receiver.getLatestOffset(topic, 2);
        } catch (Exception e) {
        }

        try {
            receiver.receive(topic, 2, 0, 1);
        } catch (Exception e) {
        }

        try {
            receiver.receive("test", 0, 0, 1);
        } catch (Exception e) {
        }

        try {
            receiver.receiveWithKey(topic, 2, 1, 1);
        } catch (Exception e) {
        }

        try {
            receiver.receiveWithKey("test", 0, 1, 1);
        } catch (Exception e) {
        }

        receiver.getPartitionCount(topic);

        receiver.shutDown();

        KafkaMessageReceiver.logger.info("test");
    }

}
