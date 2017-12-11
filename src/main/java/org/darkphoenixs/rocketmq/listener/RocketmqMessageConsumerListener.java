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

import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.message.MessageExt;
import org.darkphoenixs.mq.codec.MQMessageDecoder;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.rocketmq.consumer.AbstractConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Title: RocketmqMessageConsumerListener</p>
 * <p>Description: Rocketmq消息监听器实现类</p>
 *
 * @param <T> the type parameter
 * @author Victor
 * @version 1.0
 * @see RocketmqMessageListener
 * @since 2017 /12/10
 */
public class RocketmqMessageConsumerListener<T> extends RocketmqMessageListener<T> {

    /**
     * The Logger.
     */
    protected Logger logger = LoggerFactory.getLogger(RocketmqMessageConsumerListener.class);

    private MQMessageDecoder<T> messageDecoder;

    private AbstractConsumer<T> consumer;

    private BATCH batch = BATCH.NON_BATCH;

    private MODEL model = MODEL.MODEL_1;

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
     * Gets consumer.
     *
     * @return the consumer
     */
    public AbstractConsumer<T> getConsumer() {
        return consumer;
    }

    /**
     * Sets consumer.
     *
     * @param consumer the consumer
     */
    public void setConsumer(AbstractConsumer<T> consumer) {
        this.consumer = consumer;
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
        this.batch = BATCH.valueOf(batch);
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
        this.model = MODEL.valueOf(model);
    }

    @Override
    public void onMessage(List<T> messages) throws MQException {

        if (consumer != null)

            consumer.receive(messages);
        else
            throw new MQException("Consumer is null !");

        logger.debug("Consume Success, Message size: " + messages.size());
    }

    @Override
    public MessageListener getMessageListener() {

        MessageListener messageListener = null;

        switch (model) {

            case MODEL_1:

                messageListener = messageListenerOrderly;

                break;

            case MODEL_2:

                messageListener = messageListenerConcurrently;

                break;
        }
        return messageListener;
    }

    @Override
    public void onMessage(T message) throws MQException {

        if (consumer != null)

            consumer.receive(message);
        else
            throw new MQException("Consumer is null !");

        logger.debug("Consume Success, Message : " + message);
    }

    /**
     * consume.
     *
     * @param messages messages
     * @throws MQException
     */
    private void consume(List<MessageExt> messages) throws MQException {

        switch (batch) {

            case BATCH:

                List<byte[]> byteList = new ArrayList<byte[]>();

                for (MessageExt message : messages)

                    byteList.add(message.getBody());

                onMessage(messageDecoder.batchDecode(byteList));

                break;

            case NON_BATCH:

                for (MessageExt message : messages)

                    onMessage(messageDecoder.decode(message.getBody()));

                break;
        }
    }


    /**
     * The Message listener concurrently.
     */
    protected MessageListenerConcurrently messageListenerConcurrently = new MessageListenerConcurrently() {

        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext consumeConcurrentlyContext) {

            try {
                consume(messages);

                logger.debug("Consume Success: " + messages);

            } catch (Exception e) {

                logger.error("Consume failed !", e);

                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    };

    /**
     * The Message listener orderly.
     */
    protected MessageListenerOrderly messageListenerOrderly = new MessageListenerOrderly() {

        @Override
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> messages, ConsumeOrderlyContext consumeOrderlyContext) {

            try {
                consume(messages);

                logger.debug("Consume Success: " + messages);

            } catch (Exception e) {

                logger.error("Consume failed !", e);

                return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }

            return ConsumeOrderlyStatus.SUCCESS;
        }
    };

    /**
     * The enum Model.
     */
    public enum MODEL {

        /**
         * Model 1 model.
         */
        MODEL_1,
        /**
         * Model 2 model.
         */
        MODEL_2
    }

    /**
     * The enum Batch.
     */
    public enum BATCH {

        /**
         * non-batch consumer.
         */
        NON_BATCH,
        /**
         * batch consumer.
         */
        BATCH
    }
}
