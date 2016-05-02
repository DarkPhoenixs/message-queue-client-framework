package org.darkphoenixs.kafka.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;

import org.I0Itec.zkclient.ZkClient;
import org.darkphoenixs.kafka.pool.KafkaMessageReceiverPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KafkaMessageReceiverImplTest {

	private int brokerId = 0;
	private String topic = "QUEUE.TEST";
	private String zkConnect;
	private EmbeddedZookeeper zkServer;
	private ZkClient zkClient;
	private KafkaServer kafkaServer;
	private int port;
	private Properties kafkaProps;

	@Before
	public void before() {
		zkConnect = TestZKUtils.zookeeperConnect();
		zkServer = new EmbeddedZookeeper(zkConnect);
		zkClient = new ZkClient(zkServer.connectString(), 30000, 30000,
				ZKStringSerializer$.MODULE$);

		// setup Broker
		port = TestUtils.choosePort();
		kafkaProps = TestUtils.createBrokerConfig(brokerId, port, true);

		KafkaConfig config = new KafkaConfig(kafkaProps);
		Time mock = new MockTime();
		kafkaServer = TestUtils.createServer(config, mock);

		// create topic
		TopicCommand.TopicCommandOptions options = new TopicCommand.TopicCommandOptions(
				new String[] { "--create", "--topic", topic,
						"--replication-factor", "1", "--partitions", "4" });

		TopicCommand.createTopic(zkClient, options);

		List<KafkaServer> servers = new ArrayList<KafkaServer>();
		servers.add(kafkaServer);
		TestUtils.waitUntilMetadataIsPropagated(
				scala.collection.JavaConversions.asScalaBuffer(servers), topic,
				0, 5000);
	}

	@After
	public void after() {
		kafkaServer.shutdown();
		zkClient.close();
		zkServer.shutdown();
	}

	@Test
	public void test() throws Exception {

		Properties consumerProps = TestUtils.createConsumerProperties(
				zkServer.connectString(), "group_1", "consumer_id", 1000);

		KafkaMessageReceiverPool<byte[], byte[]> recePool = new KafkaMessageReceiverPool<byte[], byte[]>();

		recePool.setProps(consumerProps);
		recePool.setPoolSize(10);
		recePool.setClientId("test");

		KafkaMessageReceiverImpl<byte[], byte[]> receiver = new KafkaMessageReceiverImpl<byte[], byte[]>(
				consumerProps, recePool);

		Assert.assertNotNull(receiver.getProps());
		receiver.setProps(receiver.getProps());

		Assert.assertNotNull(receiver.getPool());
		receiver.setPool(receiver.getPool());

		Assert.assertNull(receiver.getConsumer());
		receiver.setConsumer(receiver.getConsumer());

		receiver.getEarliestOffset(topic, 0);

		receiver.getLatestOffset(topic, 0);

		receiver.receive(topic, 0, 0, 1);

		receiver.receiveWithKey(topic, 0, 0, 1);

		receiver.close();
	}
}
