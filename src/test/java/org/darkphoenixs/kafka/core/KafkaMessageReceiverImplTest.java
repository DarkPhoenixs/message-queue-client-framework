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
import org.darkphoenixs.kafka.pool.KafkaMessageSenderPool;
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
		Properties kafkaProps2 = TestUtils.createBrokerConfig(1,
				TestUtils.choosePort(), true);

		KafkaConfig config = new KafkaConfig(kafkaProps);
		KafkaConfig config2 = new KafkaConfig(kafkaProps2);
		Time mock = new MockTime();
		Time mock2 = new MockTime();
		kafkaServer = TestUtils.createServer(config, mock);
		KafkaServer kafkaServer2 = TestUtils.createServer(config2, mock2);

		// create topic
		TopicCommand.TopicCommandOptions options = new TopicCommand.TopicCommandOptions(
				new String[] { "--create", "--topic", topic,
						"--replication-factor", "2", "--partitions", "2" });

		TopicCommand.createTopic(zkClient, options);

		List<KafkaServer> servers = new ArrayList<KafkaServer>();
		servers.add(kafkaServer);
		servers.add(kafkaServer2);
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
				zkServer.connectString(), "group_1", "consumer_1", 1000);

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
