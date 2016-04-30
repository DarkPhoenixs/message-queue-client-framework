package org.darkphoenixs.kafka.core;

import org.junit.Assert;
import org.junit.Test;

public class KafkaCommandTest {

	@Test
	public void test1() throws Exception {

		Assert.assertNotNull(new KafkaCommand());

		try {
			KafkaCommand.createOptions("10.0.63.10:2181", "QUEUE.TEST1", 1, 1);
		} catch (Exception e) {

			Assert.assertNotNull(e);
		}

	}

	@Test
	public void test2() throws Exception {

		try {
			KafkaCommand.listOptions("10.0.63.10:2181");

		} catch (Exception e) {

			Assert.assertNotNull(e);
		}
	}

	@Test
	public void test3() throws Exception {

		try {
			KafkaCommand.selectOptions("10.0.63.10:2181", "QUEUE.TEST1");

		} catch (Exception e) {

			Assert.assertNotNull(e);
		}
	}

	@Test
	public void test4() throws Exception {

		try {
			KafkaCommand.updateOptions("10.0.63.10:2181", "QUEUE.TEST1", 2);

		} catch (Exception e) {

			Assert.assertNotNull(e);
		}
	}

	@Test
	public void test5() throws Exception {

		try {
			KafkaCommand.updateOptions("10.0.63.10:2181", "QUEUE.TEST1", "1=1",
					"2");
		} catch (Exception e) {

			Assert.assertNotNull(e);
		}
	}

	@Test
	public void test6() throws Exception {

		try {
			KafkaCommand.updateOptions("10.0.63.10:2181", "QUEUE.TEST1", 2,
					"1=1", "2");
		} catch (Exception e) {

			Assert.assertNotNull(e);
		}
	}

	@Test
	public void test7() throws Exception {

		try {
			KafkaCommand.topicCommand("");
		} catch (Exception e) {

			Assert.assertNotNull(e);
		}
	}

	@Test
	public void test8() throws Exception {

		try {
			KafkaCommand.deleteOptions("10.0.63.10:2181", "QUEUE.TEST1");
		} catch (Exception e) {

			Assert.assertNotNull(e);
		}
	}
}
