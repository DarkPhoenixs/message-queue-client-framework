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
import org.darkphoenixs.mq.util.MQ_BATCH;
import org.darkphoenixs.mq.util.MQ_MODEL;

import java.util.HashMap;
import java.util.Map;

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
     * model
     */
    private MQ_MODEL model = MQ_MODEL.MODEL_1;

    /**
     * batch
     */
    private MQ_BATCH batch = MQ_BATCH.NON_BATCH;

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
    @Deprecated
    public KafkaDestination getDestination() {
        return destination;
    }

    /**
     * Gets model.
     *
     * @return the model
     */
    public String getModel() {
        return model.name();
    }

    /**
     * Sets model.
     *
     * @param model the model
     */
    public void setModel(String model) {
        this.model = MQ_MODEL.valueOf(model);
    }

    /**
     * Gets batch.
     *
     * @return the batch
     */
    public String getBatch() {
        return batch.name();
    }

    /**
     * Sets batch.
     *
     * @param batch the batch
     */
    public void setBatch(String batch) {
        this.batch = MQ_BATCH.valueOf(batch);
    }

    /**
     * Sets destination.
     *
     * @param destination the destination to set
     */
    @Deprecated
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
     * @param consumerRecord the record
     * @throws MQException the mq exception
     * @since 1.4.0
     */
    public void messageAdapter(ConsumerRecord<?, ?> consumerRecord) throws MQException {

        byte[] keyBytes = (byte[]) consumerRecord.key();

        byte[] valBytes = (byte[]) consumerRecord.value();

        K k = decoder.decodeKey(keyBytes);

        V v = decoder.decodeVal(valBytes);

        messageListener.onMessage(k, v);
    }

    /**
     * <p>Title: messageAdapter</p>
     * <p>Description: 消息适配方法</p>
     *
     * @param consumerRecords the records
     * @throws MQException the mq exception
     * @since 1.4.3
     */
    public void messageAdapter(ConsumerRecords<?, ?> consumerRecords) throws MQException {

        Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();

        for (ConsumerRecord<?, ?> consumerRecord : consumerRecords)

            map.put((byte[]) consumerRecord.key(), (byte[]) consumerRecord.value());

        Map<K, V> kv = decoder.batchDecode(map);

        messageListener.onMessage(kv);
    }
}

