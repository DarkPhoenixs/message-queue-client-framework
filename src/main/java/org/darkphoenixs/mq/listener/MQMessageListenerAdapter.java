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

package org.darkphoenixs.mq.listener;

import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.darkphoenixs.kafka.codec.KafkaMessageDecoder;
import org.darkphoenixs.kafka.core.KafkaMessageAdapter;
import org.darkphoenixs.kafka.listener.KafkaMessageConsumerListener;
import org.darkphoenixs.mq.codec.MQMessageDecoder;
import org.darkphoenixs.mq.consumer.MQConsumerAdapter;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.util.MQ_BATCH;
import org.darkphoenixs.mq.util.MQ_MODEL;
import org.darkphoenixs.mq.util.MQ_TYPE;
import org.darkphoenixs.rocketmq.listener.RocketmqMessageConsumerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * The type Mq message consumer listener.
 *
 * @param <T> the type parameter
 */
public class MQMessageListenerAdapter<T> implements MQMessageListener<T> {

    /**
     * The Logger.
     */
    protected Logger logger = LoggerFactory.getLogger(MQMessageListenerAdapter.class);

    /**
     * The Kafka message adapter.
     */
    protected KafkaMessageAdapter<String, T> kafkaMessageAdapter;

    /**
     * The Rocket message listener.
     */
    protected MessageListener rocketMessageListener;

    private MQMessageDecoder<T> messageDecoder;

    private MQConsumerAdapter<T> consumerAdapter;

    private MQ_TYPE type;

    private MQ_BATCH batch = MQ_BATCH.NON_BATCH;

    private MQ_MODEL model = MQ_MODEL.MODEL_1;

    /**
     * Gets message decoder.
     *
     * @return the message decoder
     */
    public MQMessageDecoder<T> getMessageDecoder() {
        return messageDecoder;
    }

    /**
     * Sets message decoder.
     *
     * @param messageDecoder the message decoder
     */
    public void setMessageDecoder(MQMessageDecoder<T> messageDecoder) {
        this.messageDecoder = messageDecoder;
    }

    /**
     * Gets consumer adapter.
     *
     * @return the consumer adapter
     */
    public MQConsumerAdapter<T> getConsumerAdapter() {
        return consumerAdapter;
    }

    /**
     * Sets consumer adapter.
     *
     * @param consumerAdapter the consumer adapter
     */
    public void setConsumerAdapter(MQConsumerAdapter<T> consumerAdapter) {
        this.consumerAdapter = consumerAdapter;
    }

    /**
     * Gets type.
     *
     * @return the type
     */
    public String getType() {
        if (type != null)
            return type.name();
        return null;
    }

    /**
     * Sets mq type.
     *
     * <p>Note: Must be last set!</p>
     *
     * @param type the mq type
     * @throws MQException the mq exception
     */
    public void setType(String type) throws MQException {
        this.type = MQ_TYPE.valueOf(type);
        this.initListener();
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

    @Override
    public void onMessage(T message) throws MQException {

        if (consumerAdapter != null)

            consumerAdapter.receive(message);
        else
            throw new MQException("MQConsumerAdapter is null !");

        logger.debug("Consume Success, Message : " + message);
    }

    /**
     * On message with key.
     *
     * @param key     the key
     * @param message the message
     * @throws MQException the mq exception
     */
    public void onMessageWithKey(String key, T message) throws MQException {

        if (consumerAdapter != null)

            consumerAdapter.receive(key, message);
        else
            throw new MQException("MQConsumerAdapter is null !");

        logger.debug("Consume Success, Key : " + key + " Message : " + message);
    }

    /**
     * On message with batch.
     *
     * @param messages the messages
     * @throws MQException the mq exception
     */
    public void onMessageWithBatch(Map<String, T> messages) throws MQException {

        if (consumerAdapter != null)

            consumerAdapter.receive(messages);
        else
            throw new MQException("MQConsumerAdapter is null !");

        logger.debug("Consume Success, Message size: " + messages.size());
    }

    /**
     * Gets kafka message adapter.
     *
     * @return the kafka message adapter
     */
    public KafkaMessageAdapter<String, T> getKafkaMessageAdapter() {

        return kafkaMessageAdapter;
    }

    /**
     * Gets rocket message listener.
     *
     * @return the rocket message listener
     */
    public MessageListener getRocketMessageListener() {

        return rocketMessageListener;
    }

    private void initListener() throws MQException {

        switch (type) {
            case KAFKA:
                if (messageDecoder == null)
                    throw new MQException("MessageDecoder must not null!");
                kafkaMessageAdapter = new KafkaMessageAdapter<String, T>();
                kafkaMessageAdapter.setBatch(getBatch());
                kafkaMessageAdapter.setModel(getModel());
                kafkaMessageAdapter.setDecoder(new KafkaMessageDecoder<String, T>() {

                    @Override
                    public String decodeKey(byte[] bytes) throws MQException {
                        if (bytes != null)
                            return new String(bytes);
                        return null;
                    }

                    @Override
                    public T decodeVal(byte[] bytes) throws MQException {
                        return messageDecoder.decode(bytes);
                    }

                    @Override
                    public List<T> batchDecode(List<byte[]> bytes) throws MQException {
                        return messageDecoder.batchDecode(bytes);
                    }

                    @Override
                    public Map<String, T> batchDecode(Map<byte[], byte[]> bytes) throws MQException {
                        Map<String, T> map = new IdentityHashMap<String, T>();
                        if (bytes != null)
                            for (Map.Entry<byte[], byte[]> entry : bytes.entrySet())
                                map.put(decodeKey(entry.getKey()), decodeVal(entry.getValue()));
                        return map;
                    }
                });
                kafkaMessageAdapter.setMessageListener(new KafkaMessageConsumerListener<String, T>() {

                    @Override
                    public void onMessage(String key, T val) throws MQException {
                        MQMessageListenerAdapter.this.onMessageWithKey(key, val);
                    }

                    @Override
                    public void onMessage(Map<String, T> messages) throws MQException {
                        MQMessageListenerAdapter.this.onMessageWithBatch(messages);
                    }
                });
                break;
            case ROCKETMQ:
                if (messageDecoder == null)
                    throw new MQException("MessageDecoder must not null!");
                RocketmqMessageConsumerListener<T> rocketmqMessageConsumerListener = new RocketmqMessageConsumerListener<T>() {

                    @Override
                    public void onMessage(String key, T val) throws MQException {
                        MQMessageListenerAdapter.this.onMessageWithKey(key, val);
                    }

                    @Override
                    public void onMessage(Map<String, T> messages) throws MQException {
                        MQMessageListenerAdapter.this.onMessageWithBatch(messages);
                    }
                };
                rocketmqMessageConsumerListener.setBatch(getBatch());
                rocketmqMessageConsumerListener.setModel(getModel());
                rocketmqMessageConsumerListener.setMessageDecoder(getMessageDecoder());
                rocketMessageListener = rocketmqMessageConsumerListener.getMessageListener();
                break;
            case ACTIVEMQ:
                // nothing
                break;
            default:
                throw new MQException("MQ type non-exist default!");
        }
    }
}
