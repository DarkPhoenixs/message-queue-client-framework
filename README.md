Message Queue Client Framework
==============================

  消息队列客户端框架（Apache Kafka &amp; Apache ActiveMQ）
  * [Release](#release)
  * [Documentation](#documentation)
  * [Producer](#producer)
  * [Consumer](#consumer)
  * [Listener](#listener)
  * [Factory](#factory)

## Release

[messagequeue-framework-1.0.jar](https://github.com/DarkPhoenixs/message-queue-client-framework/blob/master/release/messagequeue-framework-1.0.jar?raw=true) (2015-06-01) 

## Documentation

API documentation is available at

## Producer

![image](https://github.com/DarkPhoenixs/messagequeue-framework/blob/master/uml/producer.jpg)

## Consumer

![image](https://github.com/DarkPhoenixs/messagequeue-framework/blob/master/uml/consumer.jpg)

## Listener

![image](https://github.com/DarkPhoenixs/messagequeue-framework/blob/master/uml/listener.jpg)

## Factory

![image](https://github.com/DarkPhoenixs/messagequeue-framework/blob/master/uml/factory.jpg)

```xml
<!-- Message Producer Factory -->
<bean id="messageProducerFactory" class="org.darkphoenixs.mq.common.MessageProducerFactory" 
  destroy-method="destroy">
    <property name="producers"> 
        <array> 
            <ref bean="messageProducer" /> 
        </array> 
    </property> 
</bean> 
```
