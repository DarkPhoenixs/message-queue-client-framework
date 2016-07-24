package org.darkphoenixs.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.security.JaasUtils;
import org.darkphoenixs.kafka.pool.KafkaMessageSenderPool;
import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.darkphoenixs.mq.producer.Producer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import scala.Option;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "classpath*:/kafka/applicationContext-producer4test.xml" })
public class SenderWithSpringTest {

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

		final Option<java.io.File> noFile = scala.Option.apply(null);
		final Option<SecurityProtocol> noInterBrokerSecurityProtocol = scala.Option
				.apply(null);

		kafkaProps = TestUtils.createBrokerConfig(brokerId, zkConnect, false,
				false, port, noInterBrokerSecurityProtocol, noFile, true,
				false, TestUtils.RandomPort(), false, TestUtils.RandomPort(),
				false, TestUtils.RandomPort());

		kafkaProps.setProperty("auto.create.topics.enable", "true");
		kafkaProps.setProperty("num.partitions", "1");
		// We *must* override this to use the port we allocated (Kafka currently
		// allocates one port
		// that it always uses for ZK
		kafkaProps.setProperty("zookeeper.connect", this.zkConnect);
		kafkaProps.setProperty("host.name", "localhost");
		kafkaProps.setProperty("port", port + "");

		KafkaConfig config = new KafkaConfig(kafkaProps);
		Time mock = new MockTime();
		kafkaServer = TestUtils.createServer(config, mock);

		// create topic
		TopicCommand.TopicCommandOptions options = new TopicCommand.TopicCommandOptions(
				new String[] { "--create", "--topic", topic,
						"--replication-factor", "1", "--partitions", "1" });

		TopicCommand.createTopic(zkUtils, options);

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
	
	@Autowired
	private KafkaMessageSenderPool<byte[], byte[]> pool;
	
	@Autowired
	private Producer<MessageBeanImpl> producer;
	
	@Test
	public void test() throws Exception {
		
		pool.setProps(TestUtils.getProducerConfig("localhost:" + port));
		
		pool.init();
		
		for (int i = 0; i < 10; i++) {

			MessageBeanImpl message = new MessageBeanImpl();

			message.setMessageNo("MessageNo" + i);
			message.setMessageAckNo("MessageAckNo" + i);
			message.setMessageType("MessageType");
			message.setMessageContent("MessageTest".getBytes());
			message.setMessageDate(System.currentTimeMillis());

			producer.send(message);
		}
		
		pool.destroy();
	}
}
