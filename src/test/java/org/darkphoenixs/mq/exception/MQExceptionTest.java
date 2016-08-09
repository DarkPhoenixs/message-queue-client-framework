package org.darkphoenixs.mq.exception;

import org.junit.Assert;
import org.junit.Test;

public class MQExceptionTest {

    @Test
    public void test() throws Exception {

        MQException exp0 = new MQException();

        Assert.assertNull(exp0.getMessage());

        MQException exp1 = new MQException("MQException1");

        Assert.assertEquals("MQException1", exp1.getMessage());

        Throwable thr2 = new Throwable("MQException2");

        MQException exp2 = new MQException(thr2);

        Assert.assertEquals(thr2, exp2.getCause());

        Throwable thr3 = new Throwable("MQException3");

        MQException exp3 = new MQException("MQException3", thr3);

        Assert.assertEquals("MQException3", exp3.getMessage());

        Assert.assertEquals(thr3, exp3.getCause());
    }
}
