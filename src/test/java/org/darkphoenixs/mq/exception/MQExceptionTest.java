package org.darkphoenixs.mq.exception;

import org.junit.Test;

public class MQExceptionTest {

	@Test
	public void test() throws Exception {

		MQException exp0 = new MQException();
		
		exp0.getMessage();

		MQException exp1 = new MQException("MQException1");

		exp1.printStackTrace();

		Throwable thr2 = new Throwable("MQException2");

		MQException exp2 = new MQException(thr2);

		exp2.printStackTrace();

		Throwable thr3 = new Throwable("MQException3");

		MQException exp3 = new MQException("MQException3", thr3);

		exp3.printStackTrace();
	}
}
