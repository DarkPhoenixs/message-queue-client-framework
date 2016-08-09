package org.darkphoenixs.kafka.core;

import org.junit.Assert;
import org.junit.Test;

public class KafkaDestinationTest {

    @Test
    public void test() throws Exception {

        KafkaDestination destination = new KafkaDestination();

        Assert.assertNull(destination.getDestinationName());

        destination.setDestinationName("QUEUE.TEST");

        KafkaDestination destination1 = new KafkaDestination("QUEUE.TEST");

        Assert.assertNotNull(destination1.getDestinationName());
    }

}
