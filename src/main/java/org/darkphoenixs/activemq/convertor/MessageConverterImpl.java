package org.darkphoenixs.activemq.convertor;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.darkphoenixs.activemq.util.ByteUtil;
import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.support.converter.MessageConversionException;
import org.springframework.jms.support.converter.MessageConverter;

/**
 * <p>Title: MessageConverterImpl</p>
 * <p>Description: 消息转换器实现类 </p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see MessageConverter
 * @version 1.0
 */
public class MessageConverterImpl implements MessageConverter {

	/** 日志对象 */
	public static final Logger logger = LoggerFactory
			.getLogger(MessageConverterImpl.class);

	@Override
	public Message toMessage(Object obj, Session session) throws JMSException,
			MessageConversionException {

		logger.debug("Method : toMessage");

		if (!(obj instanceof MessageBeanImpl)) {

			throw new MessageConversionException("Obj is not MessageBeanImpl.");
		}

		/* Jms字节类型消息 */
		BytesMessage bytesMessage = session.createBytesMessage();

		MessageBeanImpl messageBean = (MessageBeanImpl) obj;

		bytesMessage.setStringProperty("MessageType",
				messageBean.getMessageType());
		bytesMessage.setStringProperty("MessageAckNo",
				messageBean.getMessageAckNo());
		bytesMessage.setStringProperty("MessageNo", messageBean.getMessageNo());
		bytesMessage.setLongProperty("MessageDate",
				messageBean.getMessageDate());

		bytesMessage.writeBytes(messageBean.getMessageContent());

		logger.debug("Convert Success, The Send Message No is "
				+ messageBean.getMessageNo());

		return bytesMessage;
	}

	@Override
	public Object fromMessage(Message message) throws JMSException,
			MessageConversionException {

		logger.debug("Method : fromMessage");

		if (!(message instanceof BytesMessage)) {

			throw new MessageConversionException("Message is not BytesMessage.");
		}

		/* Jms字节类型消息 */
		BytesMessage bytesMessage = (BytesMessage) message;

		MessageBeanImpl messageBean = new MessageBeanImpl();

		messageBean.setMessageAckNo(bytesMessage
				.getStringProperty("MessageAckNo"));
		messageBean.setMessageNo(bytesMessage.getStringProperty("MessageNo"));
		messageBean.setMessageType(bytesMessage
				.getStringProperty("MessageType"));
		messageBean.setMessageDate(bytesMessage.getLongProperty("MessageDate"));

		byte bytes[] = new byte[0];

		byte tmp[] = new byte[2048];

		int len = -1;

		while ((len = bytesMessage.readBytes(tmp)) != -1) {

			bytes = ByteUtil.merge(bytes, ByteUtil.sub(tmp, 0, len));
		}

		messageBean.setMessageContent(bytes);

		logger.debug("Convert Success, The Receive Message No is "
				+ messageBean.getMessageNo());

		return messageBean;
	}

}
