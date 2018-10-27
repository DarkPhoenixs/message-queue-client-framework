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

package org.darkphoenixs.benchmark.spring.producer;

import org.darkphoenixs.benchmark.utils.Counter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class ProducerApplicationRunner implements ApplicationRunner {

    public static final String message = "1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM";

    @Autowired
    private KafkaProducer producer;

    @Value("${theads}")
    private int theads;

    @Value("${counts}")
    private int counts;

    @Override
    public void run(ApplicationArguments args) throws Exception {

        for (int i = 0; i < theads; i++) {

            Thread thread = new Thread(() -> {

                Counter counter = new Counter(counts);

                counter.setBegin(System.currentTimeMillis());

                for (int l = 0; l < counter.getCount() / theads; l++) {

                    producer.send(message);
                }
                counter.setEnd(System.currentTimeMillis());

                System.err.println("send time " + counter.getTime() + " s. average send " + counter.getAverage() + "/s.");
            });

            thread.start();
        }
    }
}
