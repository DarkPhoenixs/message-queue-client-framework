/*
 * Copyright 2015-2016 Dark Phoenixs (Open-Source Organization).
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
package org.darkphoenixs.mq.message;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;

/**
 * <p>Title: AbstractMessageBean</p>
 * <p>Description: 消息对象抽象类</p>
 *
 * @author Victor.Zxy
 * @version 1.0
 * @see Serializable
 * @since 2015-06-01
 */
public abstract class AbstractMessageBean implements Serializable {

    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 95265169598188594L;

    /**
     * 消息编号
     */
    private String messageNo;

    /**
     * 消息应答编号
     */
    private String messageAckNo;

    /**
     * 消息类型
     */
    private String messageType;

    /**
     * 消息内容
     */
    private byte[] messageContent;

    /**
     * 消息时间
     */
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
     * @param messageType the messageType to set
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
     * @param messageContent the messageContent to set
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
     * @param messageDate the messageDate to set
     */
    public void setMessageDate(long messageDate) {
        this.messageDate = messageDate;
    }

    @Override
    public String toString() {

        return JSON.toJSONString(this);
    }
}
