/*
 * Copyright 2014-2024 Dark Phoenixs (Open-Source Organization).
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
package org.darkphoenixs.kafka.codec;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

import org.darkphoenixs.mq.codec.MessageDecoder;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.message.MessageBeanImpl;

/**
 * <p>Title: MessageDecoderImpl</p>
 * <p>Description: 消息解码器</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see MessageDecoder
 * @version 1.0
 */
public class MessageDecoderImpl implements MessageDecoder<MessageBeanImpl> {

	@Override
	public MessageBeanImpl decode(byte[] bytes) throws MQException {

		MessageBeanImpl message = null;

		ByteArrayInputStream bis = null;

		ObjectInputStream ois = null;

		try {
			bis = new ByteArrayInputStream(bytes);

			ois = new ObjectInputStream(bis);

			message = (MessageBeanImpl) ois.readObject();

		} catch (Exception e) {

			throw new MQException(e);

		} finally {

			try {
				if (ois != null)
					ois.close();
			} catch (Exception e) {
				throw new MQException(e);
			}

			try {
				if (bis != null)
					bis.close();
			} catch (Exception e) {
				throw new MQException(e);
			}
		}

		return message;
	}

	@Override
	public List<MessageBeanImpl> batchDecode(List<byte[]> bytes)
			throws MQException {

		List<MessageBeanImpl> list = new ArrayList<MessageBeanImpl>();

		for (byte[] bs : bytes)

			list.add(this.decode(bs));

		return list;
	}

}
