<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
## 修改点：
在flume-1.7分支上做了如下修改：
* 在taildir-source上解决了日志文件名变动后，会重复读取的问题；
* 之前taildir-source是一个event读取一行，每次批量读取N个event到channel，碰到日志里有异常栈的时候就会出问题，会出现读取的日志不完整，改成按照某个分隔符切分，比如按照时间日期，这样一个event里就是一个完整的可处理的日志，比如我们的日志格式如下：
```text
2017-06-26 21:34:13.222 INFO  com.zxx.test.flume.log.AppenderTest - (AppenderTest.java:24) - xxxxHello world!Hello world!1498484053222
2017-06-26 21:34:13.223 INFO  com.zxx.test.flume.log.AppenderTest - (AppenderTest.java:25) - yyyDDDDDDDDDDDDDDDDDDDDD1498484053222
2017-06-26 21:34:13.223 ERROR com.zxx.test.flume.log.AppenderTest - (AppenderTest.java:26) - ExceptionExceptionException:1498484053222
java.lang.RuntimeException: runtime exception
	at com.zxx.test.flume.log.AppenderTest.test(AppenderTest.java:26)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:606)
	at org.springframework.beans.factory.annotation.InitDestroyAnnotationBeanPostProcessor$LifecycleElement.invoke(InitDestroyAnnotationBeanPostProcessor.java:366)
	at org.springframework.beans.factory.annotation.InitDestroyAnnotationBeanPostProcessor$LifecycleMetadata.invokeInitMethods(InitDestroyAnnotationBeanPostProcessor.java:311)
	at org.springframework.beans.factory.annotation.InitDestroyAnnotationBeanPostProcessor.postProcessBeforeInitialization(InitDestroyAnnotationBeanPostProcessor.java:134)
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.applyBeanPostProcessorsBeforeInitialization(AbstractAutowireCapableBeanFactory.java:409)
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.initializeBean(AbstractAutowireCapableBeanFactory.java:1620)
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.doCreateBean(AbstractAutowireCapableBeanFactory.java:555)
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.createBean(AbstractAutowireCapableBeanFactory.java:483)
	at org.springframework.beans.factory.support.AbstractBeanFactory$1.getObject(AbstractBeanFactory.java:306)
	at org.springframework.beans.factory.support.DefaultSingletonBeanRegistry.getSingleton(DefaultSingletonBeanRegistry.java:230)
	at org.springframework.beans.factory.support.AbstractBeanFactory.doGetBean(AbstractBeanFactory.java:302)
	at org.springframework.beans.factory.support.AbstractBeanFactory.getBean(AbstractBeanFactory.java:197)
	at org.springframework.beans.factory.support.DefaultListableBeanFactory.preInstantiateSingletons(DefaultListableBeanFactory.java:761)
	at org.springframework.context.support.AbstractApplicationContext.finishBeanFactoryInitialization(AbstractApplicationContext.java:867)
	at org.springframework.context.support.AbstractApplicationContext.refresh(AbstractApplicationContext.java:543)
	at org.springframework.boot.SpringApplication.refresh(SpringApplication.java:693)
	at org.springframework.boot.SpringApplication.refreshContext(SpringApplication.java:360)
	at org.springframework.boot.SpringApplication.run(SpringApplication.java:303)
	at org.springframework.boot.SpringApplication.run(SpringApplication.java:1118)
	at org.springframework.boot.SpringApplication.run(SpringApplication.java:1107)
	at com.zxx.test.flume.FlumeAppenderTestApplication.main(FlumeAppenderTestApplication.java:10)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:606)
	at com.intellij.rt.execution.application.AppMain.main(AppMain.java:147)
2017-06-26 21:35:13.222 INFO  com.zxx.test.flume.log.AppenderTest - (AppenderTest.java:24) - xxxxHello world!Hello world!3452453452345
```
我们就是以这种格式2017-06-26 21:35:13.222作为分割，这样后续sink到kafka等框架后就是完整的日志。
* 在elasticsearch-sink上修改了写入es的字段，原先是直接把整个日志一股脑塞到body字段里，现在是根据\t_分割出所有的kv对，然后塞到es里。

## Welcome to Apache Flume!

Apache Flume is a distributed, reliable, and available service for efficiently
collecting, aggregating, and moving large amounts of log data. It has a simple
and flexible architecture based on streaming data flows. It is robust and fault
tolerant with tunable reliability mechanisms and many failover and recovery
mechanisms. The system is centrally managed and allows for intelligent dynamic
management. It uses a simple extensible data model that allows for online
analytic application.

The Apache Flume 1.x (NG) code line is a refactoring of the first generation
Flume to solve certain known issues and limitations of the original design.

Apache Flume is open-sourced under the Apache Software Foundation License v2.0.

## Documentation

Documentation is included in the binary distribution under the docs directory.
In source form, it can be found in the flume-ng-doc directory.

The Flume 1.x guide and FAQ are available here:

* https://cwiki.apache.org/FLUME
* https://cwiki.apache.org/confluence/display/FLUME/Getting+Started

## Contact us!

* Mailing lists: https://cwiki.apache.org/confluence/display/FLUME/Mailing+Lists
* IRC channel #flume on irc.freenode.net

Bug and Issue tracker.

* https://issues.apache.org/jira/browse/FLUME

## Compiling Flume

Compiling Flume requires the following tools:

* Oracle Java JDK 1.7
* Apache Maven 3.x

Note: The Apache Flume build requires more memory than the default configuration.
We recommend you set the following Maven options:

export MAVEN_OPTS="-Xms512m -Xmx1024m -XX:PermSize=256m -XX:MaxPermSize=512m"

To compile Flume and build a distribution tarball, run `mvn install` from the
top level directory. The artifacts will be placed under `flume-ng-dist/target/`.
