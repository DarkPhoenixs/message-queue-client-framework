package org.darkphoenixs.mq;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/kafka/applicationContext-consumer.xml" })
public class ReceiverTest {

	@Test
	public void test() throws Exception {

		Thread.sleep(5000);
	}
}
