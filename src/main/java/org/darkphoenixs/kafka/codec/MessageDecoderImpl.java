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
