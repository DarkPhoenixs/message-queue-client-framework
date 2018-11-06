/*
 * Copyright (c) 2018. Dark Phoenixs (Open-Source Organization).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.darkphoenixs.kafka.pool;

import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class KafkaMessageReceiverMonitorTest {

    @Test
    public void test() throws Exception {


        BlockingQueue blockingQueue1 = new LinkedBlockingQueue(10);
        blockingQueue1.offer(new Object());

        BlockingQueue blockingQueue2 = new LinkedBlockingQueue(10);
        blockingQueue2.offer(new Object());
        blockingQueue2.offer(new Object());
        blockingQueue2.offer(new Object());
        blockingQueue2.offer(new Object());
        blockingQueue2.offer(new Object());
        blockingQueue2.offer(new Object());

        KafkaMessageReceiverMonitor messageReceiverMonitor1 = new KafkaMessageReceiverMonitor("test", 1000, 50, blockingQueue1);
        KafkaMessageReceiverMonitor messageReceiverMonitor2 = new KafkaMessageReceiverMonitor("test", 1000, 50, blockingQueue2);

        Thread.sleep(2000);

        messageReceiverMonitor1.destroy();
        messageReceiverMonitor2.destroy();
    }

}