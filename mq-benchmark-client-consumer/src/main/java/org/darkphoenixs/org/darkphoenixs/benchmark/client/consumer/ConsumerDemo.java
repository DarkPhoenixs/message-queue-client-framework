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

package org.darkphoenixs.org.darkphoenixs.benchmark.client.consumer;

import org.darkphoenixs.benchmark.utils.Counter;
import org.darkphoenixs.mq.consumer.MQConsumerAdapter;
import org.darkphoenixs.mq.exception.MQException;

public class ConsumerDemo extends MQConsumerAdapter<String> {

    private Counter counter;

    public Counter getCounter() {
        return counter;
    }

    public void setCounter(Counter counter) {
        this.counter = counter;
    }

    @Override
    protected void doReceive(String message) throws MQException {

        if (counter.add() % counter.getCount() == 1) {

            counter.setBegin(System.currentTimeMillis());
        }

        if (counter.get() % 10000 == 0) {

            System.err.println("receive count 10000.");
        }

        if (counter.get() % counter.getCount() == 0) {

            counter.setEnd(System.currentTimeMillis());

            System.err.println("receive time " + counter.getTime()
                    + " s. average receive " + counter.getAverage() + "/s.");
        }
    }
}