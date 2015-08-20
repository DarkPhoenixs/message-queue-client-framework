Message Queue Client Framework
==============================

  消息队列客户端框架（Apache Kafka &amp; Apache ActiveMQ）
  * [Release](#release)
  * [Documentation](#documentation)
  * [Configuration](#configuration)
  * [Producer](#producer)
  * [Consumer](#consumer)
  * [Listener](#listener)
  * [Factory](#factory)

## Release

[messagequeue-framework-1.0.jar](https://github.com/darkphoenixs/message-queue-client-framework/blob/master/release/messagequeue-framework-1.0.jar?raw=true) (2015-06-01) 

## Documentation

API documentation is available at [messagequeue-framework-1.0 api] (http://htmlpreview.github.io/?https://raw.githubusercontent.com/darkphoenixs/message-queue-client-framework/master/doc/index.html)

## Configuration

 **ActiveMQ config with Spring**

 - [applicationContext-message.xm](https://github.com/darkphoenixs/message-queue-client-framework/blob/master/src/main/resources/activemq/applicationContext-message.xml)
 - [applicationContext-sender.xm](https://github.com/darkphoenixs/message-queue-client-framework/blob/master/src/main/resources/activemq/applicationContext-sender.xml)
 - [applicationContext-receiver.xml](https://github.com/darkphoenixs/message-queue-client-framework/blob/master/src/main/resources/activemq/applicationContext-receiver.xml)
 - [mq.properties](https://github.com/darkphoenixs/message-queue-client-framework/blob/master/src/main/resources/activemq/mq.properties)
***
 **Kafka config with Spring**
 
 - [applicationContext-producer.xml](https://github.com/darkphoenixs/message-queue-client-framework/blob/master/src/main/resources/kafka/applicationContext-producer.xml)
 - [applicationContext-consumer.xml](https://github.com/darkphoenixs/message-queue-client-framework/blob/master/src/main/resources/kafka/applicationContext-consumer.xml)
 - [producer.properties](https://github.com/DarkPhoenixs/message-queue-client-framework/blob/master/src/main/resources/kafka/producer.properties)
 - [consumer.properties](https://github.com/DarkPhoenixs/message-queue-client-framework/blob/master/src/main/resources/kafka/consumer.properties)

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
