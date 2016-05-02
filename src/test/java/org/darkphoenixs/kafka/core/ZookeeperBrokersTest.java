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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ZookeeperBrokersTest {

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

		ZookeeperHosts zooHosts = new ZookeeperHosts(zkServer.connectString(),
				topic);

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
