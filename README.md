# REDIC

一个简单易用的Redis缓存客户端，与Spring无缝结合，简单导入Spring环境或者配置Redic Bean即可使用，并且支持读写分离和分片。

##入门

***单节点开发配置：***

1. 导入开发测试使用的Spring环境。

>``` xml
<import resource="classpath:spring/application-context-redic-dev.xml"/>
```

2. 配置单节点属性

>``` xml
redic.cache.node.conn1=localhost:6379
```

***多节点线上配置：***

1. 在Spring环境中配置多节点的Redic Bean。

>``` xml
<bean id="redic" class="com.robert.redis.redic.Redic" init-method="init">
	<property name="nodeConnStrs">
		<list>
			<value>${redic.cache.node.conn1}</value>
			<value>${redic.cache.node.conn2}</value>
		</list>
	</property>
</bean>
```

2. 配置单节点属性

>``` xml
redic.cache.node.conn1=localhost:6379
redic.cache.node.conn2=ip:6379
```

***多节点读写分离线上配置：***

1. 在Spring环境中配置多节点的Redic Bean。

>``` xml
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

2. 配置单节点属性

>``` xml
redic.cache.readWriteSeparate=true
redic.cache.node.conn1=localhost:6379,localhost:6380
redic.cache.node.conn2=ip:6379,ip:6380
```

##TODO

1. 提供一种方式使用annotation来声明Redic，把连接节点，读写分离等属性放在annotation中声明，不用再使用spring环境。
2. 提供手工的失效转移，可提供后台控制台来提升某个从redis作为主。
3. 当前使用默认的对象池配置，需要进行压测以及调整。