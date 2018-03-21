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
import org.darkphoenixs.kafka.codec.KafkaMessageDecoderImpl;
import org.darkphoenixs.kafka.consumer.MessageConsumer;
import org.darkphoenixs.kafka.core.KafkaDestination;
import org.darkphoenixs.kafka.core.KafkaMessageAdapter;
import org.darkphoenixs.kafka.listener.KafkaMessageConsumerListener;
import org.darkphoenixs.kafka.pool.KafkaMessageReceiverPool;
import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Option;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ReceiverTest {

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
    public void test() throws Exception {

        KafkaMessageDecoderImpl messageDecoder = new KafkaMessageDecoderImpl();

        KafkaDestination kafkaDestination = new KafkaDestination(topic);

        MessageConsumer<Integer, MessageBeanImpl> MessageConsumer = new MessageConsumer<Integer, MessageBeanImpl>();

        KafkaMessageConsumerListener<Integer, MessageBeanImpl> messageConsumerListener = new KafkaMessageConsumerListener<Integer, MessageBeanImpl>();

        messageConsumerListener.setConsumer(MessageConsumer);

        KafkaMessageAdapter<Integer, MessageBeanImpl> messageAdapter = new KafkaMessageAdapter<Integer, MessageBeanImpl>();

        messageAdapter.setDecoder(messageDecoder);

        messageAdapter.setDestination(kafkaDestination);

        messageAdapter.setMessageListener(messageConsumerListener);

        KafkaMessageReceiverPool<byte[], byte[]> pool = new KafkaMessageReceiverPool<byte[], byte[]>();

        pool.setMessageAdapter(messageAdapter);

        pool.setProps(TestUtils.createConsumerProperties(zkConnect, "group_1",
                "consumer_id", 1000));

        pool.setPoolSize(10);

        pool.init();

        pool.destroy();
    }
}
