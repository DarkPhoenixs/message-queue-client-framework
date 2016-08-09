package org.darkphoenixs.activemq.consumer;

import org.junit.Assert;
import org.junit.Test;

public class MessageConsumerTest {

    @Test
    public void test() throws Exception {

        MessageConsumer<String> consumer = new MessageConsumer<String>();

        consumer.setConsumerKey("consumerKey");

        consumer.receive(consumer.getConsumerKey());

        Assert.assertEquals("consumerKey", consumer.getConsumerKey());
    }

}
