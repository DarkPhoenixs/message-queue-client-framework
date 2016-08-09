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
package org.darkphoenixs.kafka.core;

/**
 * <p>Title: KafkaDestination</p>
 * <p>Description: Kafka队列对象</p>
 *
 * @author Victor
 * @version 1.0
 * @since 2015年9月12日
 */
public class KafkaDestination {

    /**
     * destinationName
     */
    private String destinationName;

    /**
     * <p>Title: KafkaDestination</p>
     * <p>Description: KafkaDestination</p>
     */
    public KafkaDestination() {
    }

    /**
     * <p>Title: KafkaDestination</p>
     * <p>Description: KafkaDestination</p>
     *
     * @param destinationName destinationName
     */
    public KafkaDestination(String destinationName) {
        this.destinationName = destinationName;
    }

    /**
     * @return the destinationName
     */
    public String getDestinationName() {
        return destinationName;
    }

    /**
     * @param destinationName the destinationName to set
     */
    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

}
