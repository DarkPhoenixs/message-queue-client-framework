#Message Queue Client Framework

[![Organization](https://img.shields.io/badge/org-%20DarkPhoenixs-yellow.svg)](http://www.darkphoenixs.org)
[![Build Status](https://travis-ci.org/DarkPhoenixs/message-queue-client-framework.svg?branch=master)](https://travis-ci.org/DarkPhoenixs/message-queue-client-framework)
[![Codecov](https://codecov.io/gh/DarkPhoenixs/message-queue-client-framework/branch/master/graph/badge.svg)](https://codecov.io/gh/DarkPhoenixs/message-queue-client-framework)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.darkphoenixs/messagequeue-framework/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.darkphoenixs/messagequeue-framework/)
[![GitHub release](https://img.shields.io/github/release/DarkPhoenixs/message-queue-client-framework.svg)](https://github.com/DarkPhoenixs/message-queue-client-framework/releases)
[![License](https://img.shields.io/badge/license-%20Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

  A message queue client framework with Apache Kafka &amp; Apache ActiveMQ
  
  * [Maven](#maven)
  * [Documentation](#documentation)
  * [Configuration](#configuration)
  * [Producer](#producer)
  * [Consumer](#consumer)
  * [Listener](#listener)
  * [Factory](#factory)

## Maven

```xml
<dependency>
	<groupId>org.darkphoenixs</groupId>
	<artifactId>messagequeue-framework</artifactId>
	<version>x.x.x</version>
</dependency>
```
## Documentation

API documentation is available at [apidocs](http://htmlpreview.github.io/?https://raw.githubusercontent.com/DarkPhoenixs/message-queue-client-framework/master/doc/index.html).

## Configuration

**ActiveMQ config with Spring**
 
 - [applicationContext-message.xm](https://github.com/darkphoenixs/message-queue-client-framework/blob/master/src/test/resources/activemq/applicationContext-message.xml)
 - [applicationContext-sender.xm](https://github.com/darkphoenixs/message-queue-client-framework/blob/master/src/test/resources/activemq/applicationContext-sender.xml)
 - [applicationContext-receiver.xml](https://github.com/darkphoenixs/message-queue-client-framework/blob/master/src/test/resources/activemq/applicationContext-receiver.xml)
 - [mq.properties](https://github.com/darkphoenixs/message-queue-client-framework/blob/master/src/test/resources/activemq/mq.properties)

**Kafka config with Spring**
 
 - [applicationContext-producer.xml](https://github.com/darkphoenixs/message-queue-client-framework/blob/master/src/test/resources/kafka/applicationContext-producer.xml)
 - [applicationContext-consumer.xml](https://github.com/darkphoenixs/message-queue-client-framework/blob/master/src/test/resources/kafka/applicationContext-consumer.xml)
 - [producer.properties](https://github.com/DarkPhoenixs/message-queue-client-framework/blob/master/src/test/resources/kafka/producer.properties)
 - [consumer.properties](https://github.com/DarkPhoenixs/message-queue-client-framework/blob/master/src/test/resources/kafka/consumer.properties)

## Producer

![image](https://raw.githubusercontent.com/darkphoenixs/message-queue-client-framework/master/uml/producer.png)

## Consumer

![image](https://raw.githubusercontent.com/darkphoenixs/message-queue-client-framework/master/uml/consumer.png)

## Listener

![image](https://raw.githubusercontent.com/darkphoenixs/message-queue-client-framework/master/uml/listener.png)

## Factory

![image](https://raw.githubusercontent.com/darkphoenixs/message-queue-client-framework/master/uml/factory.png)

Factory config with Spring

```xml
<!-- Message Producer Factory -->
<bean id="messageProducerFactory" class="org.darkphoenixs.mq.common.MessageProducerFactory" 
  	init-method="init" destroy-method="destroy">
    <property name="producers"> 
        <array> 
            <ref bean="messageProducer" /> 
        </array> 
    </property> 
</bean> 

<!-- Message Consumer Factory  -->
<bean id="messageConsumerFactory" class="org.darkphoenixs.mq.common.MessageConsumerFactory" 
  	init-method="init" destroy-method="destroy">
    <property name="consumers"> 
        <array> 
            <ref bean="messageConsumer" /> 
        </array>
    </property> 
</bean>
```
