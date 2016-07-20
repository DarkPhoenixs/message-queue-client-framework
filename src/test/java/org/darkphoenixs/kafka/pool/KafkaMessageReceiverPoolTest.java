package org.darkphoenixs.kafka.pool;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.admin.TopicCommand;
import kafka.serializer.DefaultDecoder;
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
import org.darkphoenixs.kafka.codec.MessageDecoderImpl;
import org.darkphoenixs.kafka.codec.MessageEncoderImpl;
import org.darkphoenixs.kafka.consumer.MessageConsumer;
import org.darkphoenixs.kafka.core.KafkaDestination;
import org.darkphoenixs.kafka.core.KafkaMessageAdapter;
import org.darkphoenixs.kafka.core.KafkaMessageTemplate;
import org.darkphoenixs.kafka.listener.MessageConsumerListener;
import org.darkphoenixs.kafka.producer.MessageProducer;
import org.darkphoenixs.mq.codec.MessageDecoder;
import org.darkphoenixs.mq.codec.MessageEncoder;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;

import scala.Option;

public class KafkaMessageReceiverPoolTest {

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

		KafkaMessageReceiverPool<byte[], byte[]> pool = new KafkaMessageReceiverPool<byte[], byte[]>();

		pool.destroy();
		
		Assert.assertNotNull(pool.getThreadFactory());
		pool.setThreadFactory(new KafkaPoolThreadFactory());

		Assert.assertNotNull(pool.getProps());
		pool.setProps(new Properties());

		Assert.assertEquals(0, pool.getPoolSize());
		pool.setPoolSize(1);

		Assert.assertNull(pool.getZookeeperStr());
		pool.setZookeeperStr("");

		Assert.assertNull(pool.getClientId());
		pool.setClientId("test");

		Assert.assertTrue(pool.getAutoCommit());
		pool.setAutoCommit(false);

		Assert.assertNull(pool.getConfig());
		pool.setConfig(new DefaultResourceLoader()
				.getResource("kafka/consumer1.properties"));
		pool.setConfig(new DefaultResourceLoader()
				.getResource("kafka/consumer.properties"));

		pool.setProps(TestUtils.createConsumerProperties(
				zkConnect, "group_1", "consumer_id", 1000));

		Assert.assertSame(DefaultDecoder.class, pool.getKeyDecoderClass());
		pool.setKeyDecoderClass(DefaultDecoder.class);

		Assert.assertSame(DefaultDecoder.class, pool.getValDecoderClass());
		pool.setValDecoderClass(DefaultDecoder.class);

		Assert.assertNull(pool.getMessageAdapter());
		pool.setMessageAdapter(getAdapter());

		Assert.assertNotNull(pool.getReceiver());

		pool.getBrokerStr(topic);

		pool.getPartitionNum(topic);

		pool.init();

		Thread.sleep(5000);

		pool.destroy();

	}

	@Test
	public void test1() throws Exception {

		KafkaMessageReceiverPool<byte[], byte[]> recePool = new KafkaMessageReceiverPool<byte[], byte[]>();

		recePool.setProps(TestUtils.createConsumerProperties(
				zkConnect, "group_1", "consumer_id", 1000));

		recePool.setMessageAdapter(getAdapter());

		recePool.setAutoCommit(false);

		recePool.init();

		MessageEncoder<MessageBeanImpl> messageEncoder = new MessageEncoderImpl();

		KafkaDestination kafkaDestination = new KafkaDestination(topic);

		KafkaMessageSenderPool<byte[], byte[]> sendPool = new KafkaMessageSenderPool<byte[], byte[]>();

		sendPool.setProps(TestUtils.getProducerConfig("localhost:" + port));

		sendPool.init();

		KafkaMessageTemplate<MessageBeanImpl> messageTemplate = new KafkaMessageTemplate<MessageBeanImpl>();

		messageTemplate.setEncoder(messageEncoder);

		messageTemplate.setMessageSenderPool(sendPool);

		MessageProducer<MessageBeanImpl> messageProducer = new MessageProducer<MessageBeanImpl>();

		messageProducer.setMessageTemplate(messageTemplate);

		messageProducer.setDestination(kafkaDestination);

		messageProducer.send(getMessage());

		Thread.sleep(5000);

		sendPool.destroy();

		recePool.destroy();
	}

	@Test
	public void test2() throws Exception {

		KafkaMessageReceiverPool<byte[], byte[]> recePool = new KafkaMessageReceiverPool<byte[], byte[]>();

		recePool.setProps(TestUtils.createConsumerProperties(
				zkConnect, "group_1", "consumer_id", 1000));

		recePool.setMessageAdapter(getAdapterWishErr());

		recePool.setAutoCommit(true);

		recePool.init();

		MessageEncoder<MessageBeanImpl> messageEncoder = new MessageEncoderImpl();

		KafkaDestination kafkaDestination = new KafkaDestination(topic);

		KafkaMessageSenderPool<byte[], byte[]> sendPool = new KafkaMessageSenderPool<byte[], byte[]>();

		sendPool.setProps(TestUtils.getProducerConfig("localhost:" + port));

		sendPool.init();

		KafkaMessageTemplate<MessageBeanImpl> messageTemplate = new KafkaMessageTemplate<MessageBeanImpl>();

		messageTemplate.setEncoder(messageEncoder);

		messageTemplate.setMessageSenderPool(sendPool);

		MessageProducer<MessageBeanImpl> messageProducer = new MessageProducer<MessageBeanImpl>();

		messageProducer.setMessageTemplate(messageTemplate);

		messageProducer.setDestination(kafkaDestination);

		messageProducer.send(getMessage());

		Thread.sleep(5000);

		sendPool.destroy();

		recePool.destroy();
	}

	private KafkaMessageAdapter<MessageBeanImpl> getAdapter() {

		MessageDecoder<MessageBeanImpl> messageDecoder = new MessageDecoderImpl();

		KafkaDestination kafkaDestination = new KafkaDestination(topic);

		MessageConsumer<MessageBeanImpl> MessageConsumer = new MessageConsumer<MessageBeanImpl>();

		MessageConsumerListener<MessageBeanImpl> messageConsumerListener = new MessageConsumerListener<MessageBeanImpl>();

		messageConsumerListener.setConsumer(MessageConsumer);

		KafkaMessageAdapter<MessageBeanImpl> messageAdapter = new KafkaMessageAdapter<MessageBeanImpl>();

		messageAdapter.setDecoder(messageDecoder);

		messageAdapter.setDestination(kafkaDestination);

		messageAdapter.setMessageListener(messageConsumerListener);

		return messageAdapter;
	}

	private KafkaMessageAdapter<MessageBeanImpl> getAdapterWishErr() {

		MessageDecoder<MessageBeanImpl> messageDecoder = new MessageDecoder<MessageBeanImpl>() {

			@Override
			public MessageBeanImpl decode(byte[] bytes) throws MQException {

				throw new MQException("Test");
			}

			@Override
			public List<MessageBeanImpl> batchDecode(List<byte[]> bytes)
					throws MQException {
				throw new MQException("Test");
			}
		};

		KafkaDestination kafkaDestination = new KafkaDestination(topic);

		MessageConsumer<MessageBeanImpl> MessageConsumer = new MessageConsumer<MessageBeanImpl>();

		MessageConsumerListener<MessageBeanImpl> messageConsumerListener = new MessageConsumerListener<MessageBeanImpl>();

		messageConsumerListener.setConsumer(MessageConsumer);

		KafkaMessageAdapter<MessageBeanImpl> messageAdapter = new KafkaMessageAdapter<MessageBeanImpl>();

		messageAdapter.setDecoder(messageDecoder);

		messageAdapter.setDestination(kafkaDestination);

		messageAdapter.setMessageListener(messageConsumerListener);

		return messageAdapter;
	}

	private MessageBeanImpl getMessage() {

		MessageBeanImpl messageBean = new MessageBeanImpl();

		long date = System.currentTimeMillis();
		messageBean.setMessageNo("MessageNo");
		messageBean.setMessageType("MessageType");
		messageBean.setMessageAckNo("MessageAckNo");
		messageBean.setMessageDate(date);
		messageBean.setMessageContent("MessageContent".getBytes());

		return messageBean;
	}
}
