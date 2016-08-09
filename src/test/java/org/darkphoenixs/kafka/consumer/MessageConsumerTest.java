package org.darkphoenixs.kafka.consumer;

import org.junit.Assert;
import org.junit.Test;

public class MessageConsumerTest {

    @Test
    public void test() throws Exception {

        MessageConsumer<Integer, String> consumer = new MessageConsumer<Integer, String>();

        consumer.setConsumerKey("ProtocolId");

        consumer.receive(consumer.getConsumerKey());

        Assert.assertEquals("ProtocolId", consumer.getConsumerKey());

        consumer.receive(2, "test");
    }

}
