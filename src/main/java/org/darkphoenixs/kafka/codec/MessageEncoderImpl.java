package org.darkphoenixs.kafka.codec;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.darkphoenixs.mq.codec.MessageEncoder;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.message.MessageBeanImpl;

/**
 * <p>Title: MessageEncoderImpl</p>
 * <p>Description: 消息编码器</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see MessageEncoder
 * @version 1.0
 */
public class MessageEncoderImpl implements MessageEncoder<MessageBeanImpl> {

	@Override
	public byte[] encode(MessageBeanImpl message) throws MQException {

		ByteArrayOutputStream bos = null;

		ObjectOutputStream oos = null;

		byte[] bytes = null;

		try {
			bos = new ByteArrayOutputStream();

			oos = new ObjectOutputStream(bos);

			oos.writeObject(message);

			bytes = bos.toByteArray();

		} catch (Exception e) {

			throw new MQException(e);

		} finally {
			
			try {
				if (oos != null)
					oos.close();
			} catch (Exception e) {
				throw new MQException(e);
			}

			try {
				if (bos != null)
					bos.close();
			} catch (Exception e) {
				throw new MQException(e);
			}
		}

		return bytes;
	}

	@Override
	public List<byte[]> batchEncode(List<MessageBeanImpl> message)
			throws MQException {

		List<byte[]> list = new ArrayList<byte[]>();

		for (MessageBeanImpl messageBean : message)

			list.add(this.encode(messageBean));

		return list;
	}

}
