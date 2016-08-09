package org.darkphoenixs.kafka.pool;

import org.junit.Assert;
import org.junit.Test;

public class KafkaPoolThreadFactoryTest {

    @Test
    public void test() throws Exception {

        KafkaPoolThreadFactory threadFactory = new KafkaPoolThreadFactory();
        threadFactory.setDaemon(true);
        threadFactory.setPrefix("Test");
        threadFactory.setPriority(15);

        Thread thread = threadFactory.newThread(new Runnable() {

            @Override
            public void run() {

                System.out.println("test");
            }
        });

        threadFactory.setPriority(5);

        threadFactory.newThread(new Runnable() {

            @Override
            public void run() {

                System.out.println("test");
            }
        });

        Assert.assertEquals("Test-0", thread.getName());
        Assert.assertEquals(5, thread.getPriority());
        Assert.assertEquals(true, thread.isDaemon());

        KafkaPoolThreadFactory threadFactory1 = new KafkaPoolThreadFactory(
                "Test1");
        Assert.assertEquals("Test1", threadFactory1.getPrefix());

        KafkaPoolThreadFactory threadFactory2 = new KafkaPoolThreadFactory(2,
                false);
        Assert.assertEquals(2, threadFactory2.getPriority());
        Assert.assertEquals(false, threadFactory2.isDaemon());

        KafkaPoolThreadFactory threadFactory3 = new KafkaPoolThreadFactory(
                "Test3", false);
        Assert.assertEquals("Test3", threadFactory3.getPrefix());
        Assert.assertEquals(false, threadFactory3.isDaemon());

        KafkaPoolThreadFactory threadFactory4 = new KafkaPoolThreadFactory(
                "Test4", 4, true);
        Assert.assertEquals("Test4", threadFactory4.getPrefix());
        Assert.assertEquals(4, threadFactory4.getPriority());
        Assert.assertEquals(true, threadFactory4.isDaemon());

    }
}
