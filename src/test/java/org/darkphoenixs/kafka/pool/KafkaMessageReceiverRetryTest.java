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

package org.darkphoenixs.kafka.pool;

import kafka.message.MessageAndMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.darkphoenixs.kafka.core.KafkaMessageAdapter;
import org.darkphoenixs.mq.exception.MQException;
import org.junit.Test;

public class KafkaMessageReceiverRetryTest {

    @Test
    public void _test() throws Exception {

        KafkaMessageReceiverRetry kafkaMessageReceiverRetry = new KafkaMessageReceiverRetry("test", 10, new KafkaMessageAdapter<Object, Object>() {
            @Override
            public void messageAdapter(ConsumerRecord<?, ?> consumerRecord) throws MQException {
                throw new MQException(consumerRecord.value().toString());
            }

            @Override
            public void messageAdapter(MessageAndMetadata<?, ?> messageAndMetadata) throws MQException {
                throw new MQException(messageAndMetadata.topic());
            }
        });

        kafkaMessageReceiverRetry.receiveMessageCount(null);

        kafkaMessageReceiverRetry.receiveMessageRetry(new ConsumerRecord<Object, Object>("test1", 0, 123, "key", "val"));

        kafkaMessageReceiverRetry.destroy();
        Thread.sleep(2000);
    }

    @Test
    public void test() throws Exception {

        KafkaMessageReceiverRetry kafkaMessageReceiverRetry1 = new KafkaMessageReceiverRetry("test", 2, new KafkaMessageAdapter<Object, Object>() {
            @Override
            public void messageAdapter(ConsumerRecord<?, ?> consumerRecord) throws MQException {
                throw new MQException(consumerRecord.value().toString());
            }

            @Override
            public void messageAdapter(MessageAndMetadata<?, ?> messageAndMetadata) throws MQException {
                throw new MQException(messageAndMetadata.topic());
            }
        });

        KafkaMessageReceiverRetry kafkaMessageReceiverRetry2 = new KafkaMessageReceiverRetry("test", 2, new KafkaMessageAdapter<Object, Object>() {
            @Override
            public void messageAdapter(ConsumerRecord<?, ?> consumerRecord) throws MQException {
                System.out.println("success");
            }

            @Override
            public void messageAdapter(MessageAndMetadata<?, ?> messageAndMetadata) throws MQException {
                System.out.println("success");
            }
        });

        KafkaMessageReceiverRetry kafkaMessageReceiverRetry3 = new KafkaMessageReceiverRetry("test", 0, new KafkaMessageAdapter<Object, Object>() {
            @Override
            public void messageAdapter(ConsumerRecord<?, ?> consumerRecord) throws MQException {
                throw new MQException(consumerRecord.value().toString());
            }

            @Override
            public void messageAdapter(MessageAndMetadata<?, ?> messageAndMetadata) throws MQException {
                throw new MQException(messageAndMetadata.topic());
            }
        });

        kafkaMessageReceiverRetry1.receiveMessageRetry(new ConsumerRecord<Object, Object>("test1", 0, 123, "key", "val"));
        kafkaMessageReceiverRetry1.receiveMessageRetry((new MessageAndMetadata<Object, Object>("test1", 1, null, 123, null, null, -1, TimestampType.CREATE_TIME)));
        kafkaMessageReceiverRetry2.receiveMessageRetry(new ConsumerRecord<Object, Object>("tes2", 0, 123, "key", "val"));
        kafkaMessageReceiverRetry2.receiveMessageRetry((new MessageAndMetadata<Object, Object>("test2", 1, null, 123, null, null, -1, TimestampType.CREATE_TIME)));
        kafkaMessageReceiverRetry3.receiveMessageRetry(new ConsumerRecord<Object, Object>("test1", 0, 123, "key", "val"));

        Thread.sleep(2000);

        kafkaMessageReceiverRetry1.destroy();
        kafkaMessageReceiverRetry2.destroy();
        kafkaMessageReceiverRetry3.destroy();

        Thread.sleep(1000);

    }

}