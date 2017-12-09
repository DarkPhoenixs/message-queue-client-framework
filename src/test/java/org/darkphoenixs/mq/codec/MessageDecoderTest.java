package org.darkphoenixs.mq.codec;

import org.darkphoenixs.mq.exception.MQException;
import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class MessageDecoderTest {

    @Test
    public void test() throws Exception {

        MessageDecoderImpl decoder = new MessageDecoderImpl();

        Assert.assertEquals("abc", decoder.decode("abc".getBytes("UTF-8")));
        Assert.assertEquals("哈哈", decoder.decode("哈哈".getBytes("UTF-8")));

        List<byte[]> bytes = new ArrayList<byte[]>();
        bytes.add("啦啦".getBytes("UTF-8"));
        bytes.add("哈哈".getBytes("UTF-8"));

        Assert.assertNotEquals("哈哈", decoder.batchDecode(bytes).get(0));
        Assert.assertEquals("哈哈", decoder.batchDecode(bytes).get(1));
    }

    private class MessageDecoderImpl implements MQMessageDecoder<String> {

        @Override
        public String decode(byte[] bytes) throws MQException {

            try {
                return new String(bytes, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public List<String> batchDecode(List<byte[]> bytes) throws MQException {

            List<String> list = new ArrayList<String>();

            for (byte[] bs : bytes) {

                list.add(decode(bs));
            }

            return list;
        }

    }
}
