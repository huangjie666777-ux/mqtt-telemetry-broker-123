# MQTT 3.1.1 本机遥测消息代理

基于 Java 17 + Netty 4.1.118 的内存态 MQTT 3.1.1 代理，供传感器与监测程序本机联调。

## 能力边界

- 仅监听 127.0.0.1，端口可配（默认 1883），状态全部在内存
- 仅支持 CleanSession=1、QoS 0/1；拒绝其他协议版本与 QoS 2；无认证/TLS/集群
- 支持 CONNECT / SUBSCRIBE / UNSUBSCRIBE / PUBLISH / PUBACK / PINGREQ / DISCONNECT
- 主题过滤器支持 +、# 与 $ 系统主题规则；发布主题禁止通配符
- 重叠过滤器同客户端只投递一次，QoS 取发布值与匹配订阅最大值的较小者
- RETAIN 每主题存最新载荷，零字节载荷清除；新订阅回放 RETAIN=1，实时投递 RETAIN=0
- KeepAlive 1.5 倍超时判定失联并发布遗嘱；正常 DISCONNECT 不发遗嘱
- 同 ClientID 新连接接管旧连接；断开释放订阅与在途包 ID
- 报文上限 256KB、每客户端在途 64、订阅 128 条；慢客户端断开处理，不拖住其他连接

## 构建与启动

    mvn test
    mvn exec:java -Dexec.mainClass=com.example.broker.MqttBroker -Dexec.args="1883"

## 演示客户端

    # 订阅（监测程序）
    mvn exec:java -Dexec.mainClass=com.example.demo.DemoClient -Dexec.args="sub 1883 monitor-1 sensor/#"
    # 发布（传感器）：pub <端口> <clientId> <主题> <载荷> [qos] [retain]
    mvn exec:java -Dexec.mainClass=com.example.demo.DemoClient -Dexec.args="pub 1883 sensor-1 sensor/room1/temp 23.5 1 false"
    # 遗嘱：带遗嘱连接后 halt 进程模拟宕机，订阅方收到遗嘱
    mvn exec:java -Dexec.mainClass=com.example.demo.DemoClient -Dexec.args="will 1883 sensor-9 status/sensor-9"

## 代码结构

- broker/MqttBroker.java 入口与 Netty 装配（MqttDecoder 处理半包粘包）
- broker/MqttServerHandler.java 协议接入与报文分发
- broker/TopicRouter.java 主题路由与投递（去重、QoS 取小、慢客户端隔离）
- broker/Session.java / SessionManager.java 会话、包 ID、接管
- broker/RetainedStore.java 保留消息存储
- broker/Topics.java 主题校验与 +/#/$ 匹配
- demo/DemoClient.java 发布/订阅/遗嘱演示
