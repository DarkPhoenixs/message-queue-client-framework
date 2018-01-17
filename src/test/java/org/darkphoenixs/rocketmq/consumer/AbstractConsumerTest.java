/*
 * Copyright (c) 2017. Dark Phoenixs (Open-Source Organization).
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

package org.darkphoenixs.rocketmq.consumer;

import org.darkphoenixs.mq.exception.MQException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AbstractConsumerTest {

    @Test
    public void test() throws Exception {

        AbstractConsumer<String> abstractConsumer1 = new AbstractConsumer<String>() {};

        abstractConsumer1.receive("key", "test");

        abstractConsumer1.receive("test");

        abstractConsumer1.receive(Collections.singletonList("test"));

        abstractConsumer1.receive(Collections.singletonMap("key", "test"));

        AbstractConsumer<String> abstractConsumer2 = new AbstractConsumer<String>() {

            @Override
            protected void doReceive(String message) throws MQException {

                throw new MQException("test");
            }

            @Override
            protected void doReceive(List<String> messages) throws MQException {

                throw new MQException("test");
            }

            @Override
            protected void doReceive(String key, String message) throws MQException {
                throw new MQException("test");
            }

            @Override
            protected void doReceive(Map<String, String> messages) throws MQException {
                throw new MQException("test");
            }
        };

        try {
            abstractConsumer2.receive("test");
        } catch (MQException e) {
            Assert.assertNotNull(e);
        }

        try {
            abstractConsumer2.receive("key", "test");
        } catch (MQException e) {
            Assert.assertNotNull(e);
        }

        try {
            abstractConsumer2.receive(Collections.singletonList("test"));
        } catch (MQException e) {
            Assert.assertNotNull(e);
        }

        try {
            abstractConsumer2.receive(Collections.singletonMap("key", "test"));
        } catch (MQException e) {
            Assert.assertNotNull(e);
        }
    }
}