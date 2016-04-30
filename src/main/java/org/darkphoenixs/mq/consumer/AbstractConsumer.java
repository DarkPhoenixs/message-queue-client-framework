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
package org.darkphoenixs.mq.consumer;

import org.darkphoenixs.mq.exception.MQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: AbstractConsumer</p>
 * <p>Description: 消费者抽象类</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @see Consumer
 * @version 1.0
 */
public abstract class AbstractConsumer<T> implements Consumer<T> {

	/** logger */
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	/** protocolId */
	private String protocolId;

	/** iframeName */
	private String iframeName;

	/** tabName */
	private String tabName;

	/** funcName */
	private String funcName;

	/**
	 * @return the protocolId
	 */
	public String getProtocolId() {
		return protocolId;
	}

	/**
	 * @param protocolId
	 *            the protocolId to set
	 */
	public void setProtocolId(String protocolId) {
		this.protocolId = protocolId;
	}

	/**
	 * @return the iframeName
	 */
	public String getIframeName() {
		return iframeName;
	}

	/**
	 * @param iframeName
	 *            the iframeName to set
	 */
	public void setIframeName(String iframeName) {
		this.iframeName = iframeName;
	}

	/**
	 * @return the tabName
	 */
	public String getTabName() {
		return tabName;
	}

	/**
	 * @param tabName
	 *            the tabName to set
	 */
	public void setTabName(String tabName) {
		this.tabName = tabName;
	}

	/**
	 * @return the funcName
	 */
	public String getFuncName() {
		return funcName;
	}

	/**
	 * @param funcName
	 *            the funcName to set
	 */
	public void setFuncName(String funcName) {
		this.funcName = funcName;
	}

	@Override
	public void receive(T message) throws MQException {

		try {
			doReceive(message);

		} catch (Exception e) {

			throw new MQException(e);
		}

		logger.debug("Receive Success, ConsumerKey : " + this.getConsumerKey()
				+ " , Message : " + message);
	}

	@Override
	public String getConsumerKey() throws MQException {

		return getProtocolId();
	}

	/**
	 * <p>Title: doReceive</p>
	 * <p>Description: 消息接收方法</p>
	 * 
	 * @param message 消息
	 * @throws MQException MQ异常
	 */
	protected abstract void doReceive(T message) throws MQException;

}
