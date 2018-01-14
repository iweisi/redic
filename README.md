# REDIC

## Redic是什么？

Redic是一个简单易用的Redis缓存客户端，与Spring无缝结合，简单导入Spring环境或者配置Redic Bean即可使用，并且支持读写分离和分片。

## 如何使用Redic？

### 1. 配置

单节点开发配置、多节点线上配置、多节点读写分离线上配置参考如下。

#### 1). 单节点开发配置

- 导入开发测试使用的Spring环境。

    ```xml
    <import resource="classpath:spring/application-context-redic-dev.xml"/>
    ```

- 配置单节点属性

    ```xml
    redic.cache.node.conn1=localhost:6379
    ```

#### 2). 多节点线上配置

- 在Spring环境中配置多节点的Redic Bean。

    ```xml
    <bean id="redic" class="com.robert.redis.redic.Redic" init-method="init">
        <property name="nodeConnStrs">
            <list>
                <value>${redic.cache.node.conn1}</value>
                <value>${redic.cache.node.conn2}</value>
            </list>
        </property>
    </bean>
    ```

- 配置单节点属性

    ```xml
    redic.cache.node.conn1=localhost:6379
    redic.cache.node.conn2=ip:6379
    ```

#### 3). 多节点读写分离线上配置

- 在Spring环境中配置多节点的Redic Bean。

    ```xml
    <bean id="redic" class="com.robert.redis.redic.Redic" init-method="init">
        <property name="readWriteSeparate" value=${redic.cache.readWriteSeparate}>
        <property name="nodeConnStrs">
            <list>
                <value>${redic.cache.node.conn1}</value>
                <value>${redic.cache.node.conn2}</value>
            </list>
        </property>
    </bean>
    ```

- 配置单节点属性

    ```xml
    redic.cache.readWriteSeparate=true
    redic.cache.node.conn1=localhost:6379,localhost:6380
    redic.cache.node.conn2=ip:6379,ip:6380
    ```

### 2. 使用

```java
Redic redic = (Redic) applicationContext.getBean("redic");   
redic.set("name", "robert");
AssertJUnit.assertEquals("robert", redic.get("name"));
```

## 联系开发者艳鹏

> 微信：robert_lyp