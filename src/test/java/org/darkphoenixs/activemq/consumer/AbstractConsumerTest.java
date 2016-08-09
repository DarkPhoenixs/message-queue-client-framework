/*
 * Copyright 2015-2016 Dark Phoenixs (Open-Source Organization).
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
package org.darkphoenixs.activemq.consumer;

import org.darkphoenixs.mq.exception.MQException;
import org.junit.Assert;
import org.junit.Test;

public class AbstractConsumerTest {

    @Test
    public void test() throws Exception {

        ConsumerImpl consumer = new ConsumerImpl();

        consumer.setConsumerKey("ProtocolId");

        consumer.receive(consumer.getConsumerKey());

        Assert.assertEquals(consumer.getConsumerKey(), consumer.getConsumerKey());

        ConsumerImpl2 consumer2 = new ConsumerImpl2();

        try {
            consumer2.receive(null);

        } catch (Exception e) {

            Assert.assertTrue(e instanceof MQException);
        }

    }

    private class ConsumerImpl extends AbstractConsumer<String> {

        @Override
        protected void doReceive(String message) throws MQException {
            System.out.println(message);
        }
    }

    private class ConsumerImpl2 extends AbstractConsumer<String> {

        @Override
        protected void doReceive(String message) throws MQException {

            throw new MQException("Test");
        }
    }
}
