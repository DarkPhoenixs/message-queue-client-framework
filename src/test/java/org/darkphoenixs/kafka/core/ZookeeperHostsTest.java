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

public class ZookeeperHostsTest {

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
						"--replication-factor", "1", "--partitions", "1" });

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

		Assert.assertEquals(KafkaConstants.DEFAULT_ZK_ROOT,
				zooHosts.getBrokerZkPath());

		Assert.assertEquals(zkServer.connectString(), zooHosts.getBrokerZkStr());

		Assert.assertEquals(topic, zooHosts.getTopic());

		ZookeeperHosts zooHosts1 = new ZookeeperHosts(zkServer.connectString(),
				KafkaConstants.DEFAULT_ZK_ROOT, topic);

		Assert.assertEquals(KafkaConstants.DEFAULT_REFRESH_FRE_SEC,
				zooHosts1.getRefreshFreqSecs());

		zooHosts1.setRefreshFreqSecs(0);
		zooHosts1.setTopic(topic);
		zooHosts1.setBrokerZkPath(KafkaConstants.DEFAULT_ZK_ROOT);
		zooHosts1.setBrokerZkStr(zkServer.connectString());
	}
}
