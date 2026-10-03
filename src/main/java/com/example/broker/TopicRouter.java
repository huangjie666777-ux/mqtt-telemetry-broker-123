package com.example.broker;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 主题路由：把发布消息投递给所有匹配会话；同客户端去重，QoS 取较小者。 */
public final class TopicRouter {

    private static final Logger log = LoggerFactory.getLogger(TopicRouter.class);

    private final SessionManager sessionManager;
    private final BrokerConfig config;

    public TopicRouter(SessionManager sessionManager, BrokerConfig config) {
        this.sessionManager = sessionManager;
        this.config = config;
    }

    /**
     * 投递一条发布消息。
     *
     * @param retainFlag 出站 RETAIN 标志：实时投递为 false，保留回放为 true
     */
    public void route(String topic, byte[] payload, MqttQoS publishQos, boolean retainFlag) {
        for (Session session : sessionManager.all()) {
            MqttQoS maxSub = null;
            for (var entry : session.subscriptions().entrySet()) {
                if (Topics.matches(entry.getKey(), topic)) {
                    MqttQoS q = entry.getValue();
                    if (maxSub == null || q.value() > maxSub.value()) {
                        maxSub = q;
                    }
                }
            }
            if (maxSub == null) {
                continue;
            }
            MqttQoS outQos = MqttQoS.valueOf(Math.min(publishQos.value(), maxSub.value()));
            deliver(session, topic, payload, outQos, retainFlag);
        }
    }

    /** 向单会话投递（保留回放也走这里）；慢客户端直接断开，不拖住他人。 */
    public void deliver(Session session, String topic, byte[] payload, MqttQoS qos, boolean retainFlag) {
        var channel = session.channel();
        if (!channel.isActive()) {
            return;
        }
        if (!channel.isWritable() || session.inflightCount() >= config.maxInflightPerClient()) {
            log.warn("clientId={} 发送缓冲或在途超限，判定为慢客户端并断开", session.clientId());
            channel.close();
            return;
        }
        int packetId = 0;
        if (qos == MqttQoS.AT_LEAST_ONCE) {
            packetId = session.nextPacketId();
            if (packetId < 0) {
                log.warn("clientId={} 出站包 ID 耗尽，断开", session.clientId());
                channel.close();
                return;
            }
        }
        MqttFixedHeader header = new MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, retainFlag, 0);
        MqttPublishMessage msg = new MqttPublishMessage(header,
                new MqttPublishVariableHeader(topic, packetId), Unpooled.wrappedBuffer(payload));
        channel.writeAndFlush(msg);
    }
}
