package com.example.mqtt.broker;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 业务核心：连接接管、订阅路由、保留消息、遗嘱发布。
 * 不依赖 Netty 编解码细节，仅操作 Session/Registry/Store。
 */
public final class Broker {
    private static final Logger log = LoggerFactory.getLogger(Broker.class);

    private final SessionRegistry registry = new SessionRegistry();
    private final RetainedStore retainedStore = new RetainedStore();

    public SessionRegistry registry() {
        return registry;
    }

    public RetainedStore retainedStore() {
        return retainedStore;
    }

    /** 新连接接入；同 ClientID 旧连接被接管关闭（旧连接清理不得影响新会话）。 */
    public void attach(Session session) {
        Session old = registry.register(session);
        if (old != null && old.channel() != session.channel()) {
            log.info("clientId {} takeover: closing old connection", session.clientId());
            old.markGraceful(); // 接管不发布旧遗嘱
            old.channel().close();
        }
    }

    /**
     * 连接断开清理：仅当注册表仍指向该会话时移除；
     * 非正常断开发布遗嘱；释放订阅与在途消息（会话对象整体丢弃）。
     */
    public void detach(Session session) {
        boolean removed = registry.removeIfSame(session);
        if (removed && !session.isGraceful() && session.will() != null) {
            Session.WillMessage will = session.will();
            log.info("publishing will of {} to {}", session.clientId(), will.topic());
            publish(session, will.topic(), will.payload(), will.qos(), will.retain());
        }
    }

    /** 处理入站发布：更新保留存储并路由给匹配订阅者。 */
    public void publish(Session from, String topic, byte[] payload, int qos, boolean retain) {
        if (retain) {
            retainedStore.update(topic, payload, qos);
        }
        route(topic, payload, qos, false);
    }

    /**
     * 路由：同一客户端多个重叠过滤器只投递一次，QoS 取发布值与匹配订阅最大值的较小者。
     */
    void route(String topic, byte[] payload, int publishQos, boolean retainedReplay) {
        for (Session session : registry.all()) {
            int best = -1;
            for (Map.Entry<String, Integer> sub : session.subscriptions().entrySet()) {
                if (Topics.matches(sub.getKey(), topic)) {
                    best = Math.max(best, sub.getValue());
                }
            }
            if (best < 0) {
                continue;
            }
            int outQos = Math.min(publishQos, best);
            deliver(session, topic, payload, outQos, retainedReplay);
        }
    }

    /** 新订阅建立后回放匹配的保留消息（RETAIN=1）。 */
    public void replayRetained(Session session, String filter) {
        Map<String, RetainedStore.RetainedMessage> matched = new HashMap<>();
        retainedStore.forEachMatch(filter, (topic, msg) -> matched.put(topic, msg));
        for (RetainedStore.RetainedMessage msg : matched.values()) {
            int outQos = Math.min(msg.qos(), session.subscriptions().getOrDefault(filter, 0));
            deliver(session, msg.topic(), msg.payload(), outQos, true);
        }
    }

    private void deliver(Session session, String topic, byte[] payload, int qos, boolean retainedReplay) {
        MqttMessageBuilders.PublishBuilder builder = MqttMessageBuilders.publish()
                .topicName(topic)
                .payload(Unpooled.wrappedBuffer(payload))
                .qos(MqttQoS.valueOf(qos))
                .retained(retainedReplay);
        if (qos > 0) {
            int packetId = session.nextOutboundPacketId();
            builder.messageId(packetId);
        }
        MqttPublishMessage msg = builder.build();
        if (qos > 0) {
            session.addInflight(msg.variableHeader().packetId(), msg);
        }
        if (!session.write(msg)) {
            log.warn("slow client {} exceeded pending limit, closing", session.clientId());
            session.channel().close();
        }
    }
}
