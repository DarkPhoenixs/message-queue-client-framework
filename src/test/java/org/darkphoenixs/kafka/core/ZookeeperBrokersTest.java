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
import java.util.Properties;

public class ZookeeperBrokersTest {

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
                        "--replication-factor", "1", "--partitions", "4"});

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

        ZookeeperHosts zooHosts = new ZookeeperHosts(zkConnect, topic);

        final ZookeeperBrokers brokers = new ZookeeperBrokers(zooHosts);

        brokers.brokerPath();

        brokers.partitionPath();

        System.out.println(brokers.getBrokerInfo());

        Assert.assertEquals(
                "localhost:9092",
                brokers.getBrokerHost("{\"host\":\"localhost\", \"jmx_port\":9999, \"port\":9092, \"version\":1 }"
                        .getBytes("UTF-8")));

        try {
            brokers.getBrokerHost("1".getBytes());
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        Assert.assertEquals(0, brokers.getLeaderFor(0));

        try {
            brokers.getLeaderFor(-1);
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        Assert.assertEquals(4, brokers.getNumPartitions());

        try {
            brokers.close();
            brokers.getNumPartitions();
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        try {
            brokers.getLeaderFor(0);
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        try {
            brokers.getBrokerInfo();
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }
    }
}
