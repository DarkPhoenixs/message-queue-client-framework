package org.darkphoenixs.kafka.pool;

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
import org.darkphoenixs.kafka.core.KafkaMessageSender;
import org.darkphoenixs.kafka.core.ZookeeperHosts;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import scala.Option;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

public class KafkaMessageSenderPoolTest {

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
    public void test_0() throws Exception {

        KafkaMessageSenderPool<byte[], byte[]> pool = new KafkaMessageSenderPool<byte[], byte[]>();

        Assert.assertFalse(pool.isRunning());

        Assert.assertNull(pool.getThreadFactory());
        pool.setThreadFactory(new KafkaPoolThreadFactory());

        Assert.assertNotNull(pool.getProps());
        pool.setProps(new Properties());

        Assert.assertEquals(0, pool.getPoolSize());
        pool.setPoolSize(70);

        Assert.assertNull(pool.getClientId());
        pool.setClientId("test");

        Assert.assertNull(pool.getBrokerStr());
        pool.setBrokerStr("");

        Assert.assertNull(pool.getConfig());
        pool.setConfig(new DefaultResourceLoader()
                .getResource("kafka/producer1.properties"));
        pool.setConfig(new DefaultResourceLoader()
                .getResource("kafka/producer.properties"));

        pool.setProps(TestUtils.getProducerConfig("localhost:" + port));

        pool.setZkhosts(new ZookeeperHosts(zkConnect, topic));

        pool.init();

        Assert.assertTrue(pool.isRunning());

        Assert.assertNotNull(pool.getSender());

        Assert.assertNotNull(pool.getSender());

        KafkaMessageSender<byte[], byte[]> sender = pool.getSender();

        pool.returnSender(sender);
        pool.returnSender(sender);

        pool.destroy();

        Assert.assertFalse(pool.isRunning());
    }

    @Test
    public void test_1() throws Exception {

        KafkaMessageSenderPool<byte[], byte[]> pool1 = new KafkaMessageSenderPool<byte[], byte[]>();

        pool1.getSender();

        pool1.setConfig(new DefaultResourceLoader()
                .getResource("kafka/producer.properties"));

        pool1.setProps(TestUtils.getProducerConfig("localhost:" + port));
        pool1.setPoolSize(2);
        pool1.init();
        pool1.destroy();

        KafkaMessageSenderPool<byte[], byte[]> pool2 = new KafkaMessageSenderPool<byte[], byte[]>();

        pool2.setConfig(new DefaultResourceLoader()
                .getResource("kafka/producer.properties"));

        pool2.setProps(TestUtils.getProducerConfig("localhost:" + port));
        pool2.setPoolSize(0);
        pool2.init();
        pool2.destroy();

    }

    @Test
    public void test_2() throws Exception {

        KafkaMessageSenderPool<byte[], byte[]> pool = new KafkaMessageSenderPool<byte[], byte[]>();

        pool.setConfig(new DefaultResourceLoader()
                .getResource("kafka/producer.properties"));

        pool.setProps(TestUtils.getProducerConfig("localhost:" + port));

        pool.setZkhosts(new ZookeeperHosts(zkConnect, topic));

        pool.init();

        for (int i = 0; i < 70; i++) {

            try {
                pool.getSender();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        pool.destroy();

        KafkaMessageSenderPoolDemo poolDemo = new KafkaMessageSenderPoolDemo();

        poolDemo.getSender();

        poolDemo.init();

        poolDemo.getSender();

        poolDemo.destroy();
    }


    class KafkaMessageSenderPoolDemo extends KafkaMessageSenderPool<byte[], byte[]> {

        @Override
        public synchronized void init() {

            this.freeSender = new Semaphore(1);

            this.queue = new LinkedBlockingQueue<KafkaMessageSender<byte[], byte[]>>(1);

            this.pool = Executors.newFixedThreadPool(1, new KafkaPoolThreadFactory());

            this.setConfig(new DefaultResourceLoader()
                    .getResource("kafka/producer.properties"));

            this.setProps(TestUtils.getProducerConfig("localhost:" + port));

            this.setZkhosts(new ZookeeperHosts(zkConnect, topic));
        }
    }
}
