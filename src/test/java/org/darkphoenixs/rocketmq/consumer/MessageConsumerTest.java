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

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class MessageConsumerTest {

    @Test
    public void test() throws Exception {

        MessageConsumer<String> messageConsumer = new MessageConsumer<String>();

        messageConsumer.setConsumerKey("test");

        Assert.assertEquals(messageConsumer.getConsumerKey(), "test");

        messageConsumer.receive("test");

        messageConsumer.receive("key", "test");

        messageConsumer.receive(Collections.singletonList("test"));

        messageConsumer.receive(Collections.singletonMap("key", "test"));

    }
}