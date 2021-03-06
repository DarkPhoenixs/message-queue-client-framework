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

package org.darkphoenixs.mq.consumer;

import org.darkphoenixs.mq.exception.MQException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MQConsumerAdapterTest {

    MQConsumerAdapter<String> mqConsumerAdapter = new MQConsumerAdapter<String>() {
        @Override
        protected void doReceive(String message) throws MQException {
            System.out.println(message);
        }
    };

    MQConsumerAdapter<String> mqConsumerAdapter2 = new MQConsumerAdapter<String>() {
        @Override
        protected void doReceive(String message) throws MQException {

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

        @Override
        protected void doReceive(List<String> messages) throws MQException {
            throw new MQException("test");
        }
    };

    @Test
    public void getConsumerKey() {
        Assert.assertNull(mqConsumerAdapter.getConsumerKey());
    }

    @Test
    public void setConsumerKey() {
        mqConsumerAdapter.setConsumerKey("ConsumerKey");
    }

    @Test
    public void receive() {
        try {
            mqConsumerAdapter.receive("test");
        } catch (MQException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void receive1() {
        try {
            mqConsumerAdapter.receive("key", "test");
        } catch (MQException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void receive2() {
        try {
            mqConsumerAdapter.receive(Collections.singletonMap("key", "test"));
        } catch (MQException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void doReceive() {
        try {
            mqConsumerAdapter2.receive("test");
        } catch (MQException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void doReceive1() {
        try {
            mqConsumerAdapter2.receive("key", "test");
        } catch (MQException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void doReceive2() {
        try {
            mqConsumerAdapter2.receive(Collections.singletonMap("key", "test"));
        } catch (MQException e) {
            e.printStackTrace();
        }
    }
}