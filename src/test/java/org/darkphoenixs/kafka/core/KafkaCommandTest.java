package org.darkphoenixs.kafka.core;

import org.junit.Assert;
import org.junit.Test;

public class KafkaCommandTest {

	@Test
	public void test() throws Exception {

		Assert.assertNotNull(new KafkaCommand());

		Thread thread1 = new Thread(new Runnable() {

			@Override
			public void run() {
				KafkaCommand.createOptions("10.0.63.10:2181", "QUEUE.TEST1", 1,
						1);
			}
		});

		thread1.start();

		Thread thread2 = new Thread(new Runnable() {

			@Override
			public void run() {
				KafkaCommand.listOptions("10.0.63.10:2181");
			}
		});

		thread2.start();

		Thread thread3 = new Thread(new Runnable() {

			@Override
			public void run() {
				KafkaCommand.selectOptions("10.0.63.10:2181", "QUEUE.TEST1");
			}
		});

		thread3.start();

		Thread thread4 = new Thread(new Runnable() {

			@Override
			public void run() {
				KafkaCommand.updateOptions("10.0.63.10:2181", "QUEUE.TEST1", 2);
			}
		});

		thread4.start();

		Thread thread5 = new Thread(new Runnable() {

			@Override
			public void run() {
				KafkaCommand.updateOptions("10.0.63.10:2181", "QUEUE.TEST1",
						"1=1", "2");
			}
		});

		thread5.start();

		Thread thread6 = new Thread(new Runnable() {

			@Override
			public void run() {
				KafkaCommand.updateOptions("10.0.63.10:2181", "QUEUE.TEST1", 2,
						"1=1", "2");
			}
		});

		thread6.start();

		Thread thread7 = new Thread(new Runnable() {

			@Override
			public void run() {
				KafkaCommand.topicCommand("");
			}
		});

		thread7.start();
		
		Thread thread8 = new Thread(new Runnable() {

			@Override
			public void run() {
				KafkaCommand.deleteOptions("10.0.63.10:2181", "QUEUE.TEST1");
			}
		});

		thread8.start();
	}
}
