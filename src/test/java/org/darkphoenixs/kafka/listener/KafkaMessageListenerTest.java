package org.darkphoenixs.kafka.listener;

import org.junit.Test;

import java.util.HashMap;

public class KafkaMessageListenerTest {

    @Test
    public void test() throws Exception {

        KafkaMessageListenerDemo listener = new KafkaMessageListenerDemo();

        listener.onMessage("Test");

        listener.onMessage(1, "Test");

        listener.onMessage(new HashMap<Integer, String>());
    }

    class KafkaMessageListenerDemo extends KafkaMessageListener<Integer, String> {

    }
}
