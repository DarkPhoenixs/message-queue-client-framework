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

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ConsumerApplication {

    public static void main(String[] args) {

        final int theads = args.length > 0 ? Integer.valueOf(args[0]) : 10;

        final int counts = args.length > 1 ? Integer.valueOf(args[1]) : 1000000;

        String[] args2 = new String[]{"--spring.profiles.active=consumer", "--theads=" + theads, "--counts=" + counts};

        SpringApplication.run(ConsumerApplication.class, args2);
    }

}
