package org.darkphoenixs.mq.util;

import org.junit.Assert;
import org.junit.Test;

public class ByteUtilTest {

    @Test
    public void test() throws Exception {

        Assert.assertNotNull(new ByteUtil());

        byte[] bytes1 = "test1".getBytes("UTF-8");
        byte[] bytes2 = "test2".getBytes("UTF-8");
        byte[] merge = ByteUtil.merge(bytes1, bytes2);

        Assert.assertEquals("test1test2", new String(merge, "UTF-8"));

        byte[] sub = ByteUtil.sub(merge, bytes1.length, merge.length);

        Assert.assertEquals("test2", new String(sub, "UTF-8"));
    }
}
