package org.darkphoenixs.kafka.core;

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
import org.darkphoenixs.kafka.pool.KafkaMessageReceiverPool;
import org.darkphoenixs.kafka.pool.KafkaMessageSenderPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import scala.Option;

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

	@Test
	public void test() throws Exception {

		KafkaMessageSenderPool<byte[], byte[]> sendPool = new KafkaMessageSenderPool<byte[], byte[]>();

		sendPool.setProps(TestUtils.getProducerConfig("localhost:" + port));

		sendPool.init();

		Properties properties = TestUtils
				.getProducerConfig("localhost:" + port);

		KafkaMessageSenderImpl<byte[], byte[]> sender = new KafkaMessageSenderImpl<byte[], byte[]>(
				properties, sendPool);

		Assert.assertEquals(sendPool, sender.getPool());
		sender.setPool(sendPool);

		Assert.assertNotNull(sender.getProducer());
		sender.setProducer(sender.getProducer());

		sender.send(topic, "test".getBytes());

		sender.sendWithKey(topic, "key".getBytes(), "value".getBytes());

		sender.close();

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

		Assert.assertNotNull(receiver.getProps());
		receiver.setProps(receiver.getProps());

		Assert.assertNotNull(receiver.getPool());
		receiver.setPool(receiver.getPool());

		Assert.assertNull(receiver.getConsumer());
		receiver.setConsumer(receiver.getConsumer());

		receiver.getEarliestOffset(topic, -1);

		receiver.getLatestOffset(topic, -1);

		receiver.getEarliestOffset(topic, 0);

		receiver.getLatestOffset(topic, 0);

		receiver.receive(topic, 0, 0, 1);

		receiver.receive(topic, 0, 0, 2);

		receiver.receive(topic, 0, 1, 2);

		receiver.receive(topic, 1, 0, 2);

		receiver.receiveWithKey(topic, 0, 1, 1);

		receiver.receiveWithKey(topic, 0, 1, 2);

		receiver.receiveWithKey(topic, 0, 2, 2);

		receiver.receiveWithKey(topic, 1, 1, 2);

		receiver.close();

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

		receiver.close();

		KafkaMessageReceiver.logger.info("test");
	}

}
