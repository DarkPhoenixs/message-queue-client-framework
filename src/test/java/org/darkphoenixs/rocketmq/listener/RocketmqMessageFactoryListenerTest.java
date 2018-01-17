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

package org.darkphoenixs.rocketmq.listener;

import org.darkphoenixs.mq.common.MQMessageConsumerFactory;
import org.darkphoenixs.mq.consumer.MQConsumer;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.factory.MQConsumerFactory;
import org.darkphoenixs.rocketmq.consumer.MessageConsumer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class RocketmqMessageFactoryListenerTest {

    static class Bean {

        private int id;
        private String name;
        private String type;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    @Test
    public void test() throws Exception {

        RocketmqMessageFactoryListener<Bean> factoryListener = new RocketmqMessageFactoryListener<Bean>();

        Assert.assertNull(factoryListener.getConsumerKeyField());
        Assert.assertNull(factoryListener.getConsumerFactory());

        Bean bean = null;
        try {
            factoryListener.onMessage(bean);
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        try {
            factoryListener.onMessage(new Bean());
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        try {
            factoryListener.onMessage(Collections.singletonList(new Bean()));
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        factoryListener.setConsumerFactory(MQMessageConsumerFactory.getInstance());

        try {
            factoryListener.onMessage(new Bean());
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        factoryListener.setConsumerKeyField("type");

        try {
            factoryListener.onMessage(new Bean());
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        bean = new Bean();

        bean.setType("test");

        try {
            factoryListener.onMessage(bean);
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        factoryListener.setConsumerFactory(new MQConsumerFactory() {

            @Override
            public <T> void addConsumer(MQConsumer<T> consumer) throws MQException {

            }

            @Override
            public <T> MQConsumer<T> getConsumer(String consumerKey) throws MQException {
                return new MessageConsumer<T>();
            }

            @Override
            public void init() throws MQException {

            }

            @Override
            public void destroy() throws MQException {

            }
        });

        factoryListener.onMessage(bean);
    }

    @Test
    public void test1() throws Exception {

        RocketmqMessageFactoryListener<Bean> factoryListener = new RocketmqMessageFactoryListener<Bean>();

        Assert.assertNull(factoryListener.getConsumerKeyField());
        Assert.assertNull(factoryListener.getConsumerFactory());

        Bean bean = null;
        try {
            factoryListener.onMessage(null, bean);
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        try {
            factoryListener.onMessage(null, new Bean());
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        try {
            factoryListener.onMessage(Collections.singletonMap("", new Bean()));
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        factoryListener.setConsumerFactory(MQMessageConsumerFactory.getInstance());

        try {
            factoryListener.onMessage(null, new Bean());
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        factoryListener.setConsumerKeyField("type");

        try {
            factoryListener.onMessage(null, new Bean());
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        bean = new Bean();

        bean.setType("test");

        try {
            factoryListener.onMessage(null, bean);
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }

        factoryListener.setConsumerFactory(new MQConsumerFactory() {

            @Override
            public <T> void addConsumer(MQConsumer<T> consumer) throws MQException {

            }

            @Override
            public <T> MQConsumer<T> getConsumer(String consumerKey) throws MQException {
                return new MessageConsumer<T>();
            }

            @Override
            public void init() throws MQException {

            }

            @Override
            public void destroy() throws MQException {

            }
        });

        factoryListener.onMessage(null, bean);
    }
}