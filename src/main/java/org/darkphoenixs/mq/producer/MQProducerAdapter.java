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

package org.darkphoenixs.mq.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.darkphoenixs.kafka.codec.KafkaMessageEncoder;
import org.darkphoenixs.kafka.pool.MessageSenderPool;
import org.darkphoenixs.mq.codec.MQMessageEncoder;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.util.MQ_TYPE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;

import javax.jms.Destination;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The type Mq producer adapter.
 *
 * @param <T> the type parameter
 */
public abstract class MQProducerAdapter<T> implements MQProducer<T> {

    /**
     * The Logger.
     */
    protected Logger logger = LoggerFactory.getLogger(getClass());

    /* activemq */
    private JmsTemplate activemqTemplate;
    private Destination activemqDestination;
    /* activemq */

    /* kafka */
    private MessageSenderPool<byte[], byte[]> kafkaMessageSenderPool;
    /* kafka */

    /* rocketmq */
    private DefaultMQProducer rocketmqDefaultProducer;
    private TransactionMQProducer rocketmqTransactionProducer;
    /* rocketmq */

    /* common */
    private MQMessageEncoder<T> messageEncoder;
    private String topic;
    private String producerKey;
    /* common */

    /**
     * The mq type
     */
    private MQ_TYPE type;

    /**
     * Gets activemq template.
     *
     * @return the activemq template
     */
    public JmsTemplate getActivemqTemplate() {
        return activemqTemplate;
    }

    /**
     * Sets activemq template.
     *
     * @param activemqTemplate the activemq template
     */
    public void setActivemqTemplate(JmsTemplate activemqTemplate) {
        this.activemqTemplate = activemqTemplate;
    }

    /**
     * Gets activemq destination.
     *
     * @return the activemq destination
     */
    public Destination getActivemqDestination() {
        return activemqDestination;
    }

    /**
     * Sets activemq destination.
     *
     * @param activemqDestination the activemq destination
     */
    public void setActivemqDestination(Destination activemqDestination) {
        this.activemqDestination = activemqDestination;
    }

    /**
     * Gets kafka message sender pool.
     *
     * @return the kafka message sender pool
     */
    public MessageSenderPool<byte[], byte[]> getKafkaMessageSenderPool() {
        return kafkaMessageSenderPool;
    }

    /**
     * Sets kafka message sender pool.
     *
     * @param kafkaMessageSenderPool the kafka message sender pool
     */
    public void setKafkaMessageSenderPool(MessageSenderPool<byte[], byte[]> kafkaMessageSenderPool) {
        this.kafkaMessageSenderPool = kafkaMessageSenderPool;
    }

    /**
     * Gets rocketmq default producer.
     *
     * @return the rocketmq default producer
     */
    public DefaultMQProducer getRocketmqDefaultProducer() {
        return rocketmqDefaultProducer;
    }

    /**
     * Sets rocketmq default producer.
     *
     * @param rocketmqDefaultProducer the rocketmq default producer
     */
    public void setRocketmqDefaultProducer(DefaultMQProducer rocketmqDefaultProducer) {
        this.rocketmqDefaultProducer = rocketmqDefaultProducer;
    }

    /**
     * Gets rocketmq transaction producer.
     *
     * @return the rocketmq transaction producer
     */
    public TransactionMQProducer getRocketmqTransactionProducer() {
        return rocketmqTransactionProducer;
    }

    /**
     * Sets rocketmq transaction producer.
     *
     * @param rocketmqTransactionProducer the rocketmq transaction producer
     */
    public void setRocketmqTransactionProducer(TransactionMQProducer rocketmqTransactionProducer) {
        this.rocketmqTransactionProducer = rocketmqTransactionProducer;
    }

    /**
     * Gets message encoder.
     *
     * @return the message encoder
     */
    public MQMessageEncoder<T> getMessageEncoder() {
        return messageEncoder;
    }

    /**
     * Sets message encoder.
     *
     * @param messageEncoder the message encoder
     */
    public void setMessageEncoder(MQMessageEncoder<T> messageEncoder) {
        this.messageEncoder = messageEncoder;
    }

    /**
     * Gets topic.
     *
     * @return the topic
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Sets topic.
     *
     * @param topic the topic
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * Gets mq type.
     *
     * @return the mq type
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
        this.initProducer();
    }

    @Override
    public String getProducerKey() {
        if (producerKey != null)
            return producerKey;
        return topic;
    }

    /**
     * Sets producer key.
     *
     * @param producerKey the producer key
     */
    public void setProducerKey(String producerKey) {
        this.producerKey = producerKey;
    }

    @Override
    public void send(T message) throws MQException {

        MQProducer<T> mqProducer = this.getProducerInstance();

        if (mqProducer != null)

            mqProducer.send(message);
        else
            throw new MQException("No matching MQProducer, Please check MQ_TYPE !");
    }

    /**
     * Send with key.
     *
     * @param key     the key
     * @param message the message
     * @throws MQException the mq exception
     */
    public void sendWithKey(String key, T message) throws MQException {

        MQProducer<T> mqProducer = this.getProducerInstance();

        if (mqProducer instanceof org.darkphoenixs.kafka.producer.AbstractProducer) {

            ((org.darkphoenixs.kafka.producer.AbstractProducer<String, T>) mqProducer).sendWithKey(key, message);

        } else if (mqProducer instanceof org.darkphoenixs.rocketmq.producer.AbstractProducer) {

            ((org.darkphoenixs.rocketmq.producer.AbstractProducer<T>) mqProducer).sendWithKey(key, message);

        } else if (mqProducer instanceof org.darkphoenixs.activemq.producer.AbstractProducer) {

            mqProducer.send(message);

        } else {

            throw new MQException("No matching MQProducer, Please check MQ_TYPE !");
        }
    }

    /**
     * Batch send.
     *
     * @param messages the messages
     * @throws MQException the mq exception
     */
    public void batchSend(List<T> messages) throws MQException {

        MQProducer<T> mqProducer = this.getProducerInstance();

        if (mqProducer instanceof org.darkphoenixs.kafka.producer.AbstractProducer) {

            for (T message : messages) {

                mqProducer.send(message);
            }

        } else if (mqProducer instanceof org.darkphoenixs.rocketmq.producer.AbstractProducer) {

            ((org.darkphoenixs.rocketmq.producer.AbstractProducer<T>) mqProducer).batchSend(messages);

        } else if (mqProducer instanceof org.darkphoenixs.activemq.producer.AbstractProducer) {

            for (T message : messages) {

                mqProducer.send(message);
            }

        } else {

            throw new MQException("No matching MQProducer, Please check MQ_TYPE !");
        }
    }

    /**
     * Init.
     *
     * @throws MQException the mq exception
     */
    private void initProducer() throws MQException {

        switch (type) {
            case KAFKA:
                if (topic == null || messageEncoder == null || kafkaMessageSenderPool == null)
                    throw new MQException("Topic & MessageEncoder & KafkaMessageSenderPool must not null!");

                org.darkphoenixs.kafka.producer.AbstractProducer<String, T> kafkaAbstractProducer = new org.darkphoenixs.kafka.producer.AbstractProducer<String, T>() {
                    @Override
                    protected T doSend(T message) throws MQException {
                        return MQProducerAdapter.this.doSend(message);
                    }
                };

                org.darkphoenixs.kafka.core.KafkaMessageTemplate<String, T> kafkaMessageTemplate = new org.darkphoenixs.kafka.core.KafkaMessageTemplate<String, T>();
                kafkaMessageTemplate.setMessageSenderPool(kafkaMessageSenderPool);
                kafkaMessageTemplate.setEncoder(new KafkaMessageEncoder<String, T>() {
                    @Override
                    public byte[] encodeKey(String key) throws MQException {
                        if (key != null)
                            return key.getBytes();
                        return null;
                    }

                    @Override
                    public byte[] encodeVal(T val) throws MQException {
                        return messageEncoder.encode(val);
                    }

                    @Override
                    public Map<byte[], byte[]> batchEncode(Map<String, T> messages) throws MQException {
                        Map<byte[], byte[]> map = new IdentityHashMap<byte[], byte[]>();
                        if (messages != null)
                            for (Map.Entry<String, T> entry : messages.entrySet())
                                map.put(encodeKey(entry.getKey()), encodeVal(entry.getValue()));
                        return map;
                    }
                });

                org.darkphoenixs.kafka.core.KafkaDestination kafkaDestination = new org.darkphoenixs.kafka.core.KafkaDestination(topic);

                kafkaAbstractProducer.setMessageTemplate(kafkaMessageTemplate);
                kafkaAbstractProducer.setDestination(kafkaDestination);
                kafkaAbstractProducer.setProducerKey(producerKey);
                producerConcurrentMap.put(type, kafkaAbstractProducer);
                break;
            case ROCKETMQ:
                if (topic == null || messageEncoder == null || (rocketmqDefaultProducer == null && rocketmqTransactionProducer == null))
                    throw new MQException("Topic & MessageEncoder & (RocketmqDefaultProducer | RocketmqTransactionProducer) must not null!");

                org.darkphoenixs.rocketmq.producer.AbstractProducer<T> rocketmqAbstractProducer = new org.darkphoenixs.rocketmq.producer.AbstractProducer<T>() {
                    @Override
                    protected T doSend(T message) throws MQException {
                        return MQProducerAdapter.this.doSend(message);
                    }

                    @Override
                    protected List<T> doSend(List<T> messages) throws MQException {
                        return MQProducerAdapter.this.doSend(messages);
                    }
                };

                rocketmqAbstractProducer.setTopic(topic);
                rocketmqAbstractProducer.setMessageEncoder(messageEncoder);
                rocketmqAbstractProducer.setDefaultMQProducer(rocketmqDefaultProducer);
                rocketmqAbstractProducer.setTransactionMQProducer(rocketmqTransactionProducer);
                rocketmqAbstractProducer.setProducerKey(producerKey);
                producerConcurrentMap.put(type, rocketmqAbstractProducer);
                break;
            case ACTIVEMQ:
                if (activemqDestination == null || activemqTemplate == null)
                    throw new MQException("ActivemqDestination & ActivemqTemplate must not null!");

                org.darkphoenixs.activemq.producer.AbstractProducer<T> activemqAbstractProducer = new org.darkphoenixs.activemq.producer.AbstractProducer<T>() {
                    @Override
                    protected Object doSend(T message) throws MQException {
                        return MQProducerAdapter.this.doSend(message);
                    }
                };

                activemqAbstractProducer.setDestination(activemqDestination);
                activemqAbstractProducer.setJmsTemplate(activemqTemplate);
                activemqAbstractProducer.setProducerKey(producerKey);
                producerConcurrentMap.put(type, activemqAbstractProducer);
                break;
            default:
                throw new MQException("MQ type non-exist default!");
        }
    }

    /**
     * Do send t.
     *
     * @param message the message
     * @return the t
     * @throws MQException the mq exception
     */
    protected abstract T doSend(T message) throws MQException;

    /**
     * Do send list.
     *
     * @param messages the messages
     * @return the list
     * @throws MQException the mq exception
     */
    protected List<T> doSend(List<T> messages) throws MQException {
        return messages;
    }

    /**
     * Gets producer instance.
     *
     * @return the producer instance
     */
    public MQProducer<T> getProducerInstance() {

        return producerConcurrentMap.get(type);
    }

    private final ConcurrentMap<MQ_TYPE, MQProducer<T>> producerConcurrentMap = new ConcurrentHashMap<MQ_TYPE, MQProducer<T>>();
}
