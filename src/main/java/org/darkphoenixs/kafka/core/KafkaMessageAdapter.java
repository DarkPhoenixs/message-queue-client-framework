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

import kafka.message.MessageAndMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.darkphoenixs.kafka.codec.KafkaMessageDecoder;
import org.darkphoenixs.kafka.listener.KafkaMessageListener;
import org.darkphoenixs.mq.exception.MQException;

/**
 * <p>Title: KafkaMessageAdapter</p>
 * <p>Description: Kafka消息适配器</p>
 *
 * @param <K> the type parameter
 * @param <V> the type parameter
 * @author Victor.Zxy
 * @version 1.0
 * @since 2015 -06-01
 */
public class KafkaMessageAdapter<K, V> {

    /**
     * decoder
     */
    private KafkaMessageDecoder<K, V> decoder;

    /**
     * consumerListener
     */
    private KafkaMessageListener<K, V> messageListener;

    /**
     * destination
     */
    private KafkaDestination destination;

    /**
     * Gets decoder.
     *
     * @return the decoder
     */
    public KafkaMessageDecoder<K, V> getDecoder() {
        return decoder;
    }

    /**
     * Sets decoder.
     *
     * @param decoder the decoder to set
     */
    public void setDecoder(KafkaMessageDecoder<K, V> decoder) {
        this.decoder = decoder;
    }

    /**
     * Gets message listener.
     *
     * @return the messageListener
     */
    public KafkaMessageListener<K, V> getMessageListener() {
        return messageListener;
    }

    /**
     * Sets message listener.
     *
     * @param messageListener the messageListener to set
     */
    public void setMessageListener(KafkaMessageListener<K, V> messageListener) {
        this.messageListener = messageListener;
    }

    /**
     * Gets destination.
     *
     * @return the destination
     */
    public KafkaDestination getDestination() {
        return destination;
    }

    /**
     * Sets destination.
     *
     * @param destination the destination to set
     */
    public void setDestination(KafkaDestination destination) {
        this.destination = destination;
    }

    /**
     * <p>Title: messageAdapter</p>
     * <p>Description: 消息适配方法</p>
     *
     * @param messageAndMetadata the message and metadata
     * @throws MQException the mq exception
     */
    public void messageAdapter(MessageAndMetadata<?, ?> messageAndMetadata) throws MQException {

        byte[] keyBytes = (byte[]) messageAndMetadata.key();

        byte[] valBytes = (byte[]) messageAndMetadata.message();

        K k = decoder.decodeKey(keyBytes);

        V v = decoder.decodeVal(valBytes);

        messageListener.onMessage(k, v);
    }

    /**
     * <p>Title: messageAdapter</p>
     * <p>Description: 消息适配方法</p>
     *
     * @param records the records
     * @throws MQException the mq exception
     * @since 1.4.0
     */
    public void messageAdapter(ConsumerRecords<?, ?> records) throws MQException {

        StringBuffer exceptionMsg = new StringBuffer();

        for (ConsumerRecord<?, ?> record : records) {

            try {
                byte[] keyBytes = (byte[]) record.key();

                byte[] valBytes = (byte[]) record.value();

                K k = decoder.decodeKey(keyBytes);

                V v = decoder.decodeVal(valBytes);

                messageListener.onMessage(k, v);

            } catch (Exception e) {

                exceptionMsg.append(System.getProperty("line.separator")).
                        append(" topic: " + record.topic()).
                        append(" offset: " + record.offset()).
                        append(" partition: " + record.partition()).
                        append(" Exception: " + e.getMessage());
            }
        }

        if (exceptionMsg.length() > 0)

            throw new MQException(exceptionMsg.toString());
    }
}

