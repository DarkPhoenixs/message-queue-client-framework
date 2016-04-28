/**
 * <p>Title: AbstractMessageBean.java</p>
 * <p>Description: AbstractMessageBean</p>
 * <p>Package: org.darkphoenixs.mq.message</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.mq.message;

import java.io.Serializable;

import com.alibaba.fastjson.JSON;

/**
 * <p>Title: AbstractMessageBean</p>
 * <p>Description: 消息对象抽象类</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see Serializable
 * @version 1.0
 */
public abstract class AbstractMessageBean implements Serializable {

	/** 序列化ID */
	private static final long serialVersionUID = 95265169598188594L;

	/** 消息编号 */
	private String messageNo;

	/** 消息应答编号 */
	private String messageAckNo;

	/** 消息类型 */
	private String messageType;

	/** 消息内容 */
	private byte[] messageContent;

	/** 消息时间 */
	private long messageDate;
	
	/**
	 * @return the messageNo
	 */
	public String getMessageNo() {
		return messageNo;
	}

	/**
	 * @param messageNo the messageNo to set
	 */
	public void setMessageNo(String messageNo) {
		this.messageNo = messageNo;
	}

	/**
	 * @return the messageAckNo
	 */
	public String getMessageAckNo() {
		return messageAckNo;
	}

	/**
	 * @param messageAckNo the messageAckNo to set
	 */
	public void setMessageAckNo(String messageAckNo) {
		this.messageAckNo = messageAckNo;
	}

	/**
	 * @return the messageType
	 */
	public String getMessageType() {
		return messageType;
	}

	/**
	 * @param messageType
	 *            the messageType to set
	 */
	public void setMessageType(String messageType) {
		this.messageType = messageType;
	}

	/**
	 * @return the messageContent
	 */
	public byte[] getMessageContent() {
		return messageContent;
	}

	/**
	 * @param messageContent
	 *            the messageContent to set
	 */
	public void setMessageContent(byte[] messageContent) {
		this.messageContent = messageContent;
	}

	/**
	 * @return the messageDate
	 */
	public long getMessageDate() {
		return messageDate;
	}

	/**
	 * @param messageDate
	 *            the messageDate to set
	 */
	public void setMessageDate(long messageDate) {
		this.messageDate = messageDate;
	}
	
	@Override
	public String toString() {

		return JSON.toJSONString(this);
	}
}
