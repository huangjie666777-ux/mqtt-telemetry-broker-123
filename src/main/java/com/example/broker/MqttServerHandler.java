package com.example.broker;

import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/** 协议接入层：处理 CONNECT/SUBSCRIBE/UNSUBSCRIBE/PUBLISH/PUBACK/PINGREQ/DISCONNECT。 */
public final class MqttServerHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private static final Logger log = LoggerFactory.getLogger(MqttServerHandler.class);
    private static final String IDLE_HANDLER = "keepAliveIdle";

    private final BrokerConfig config;
    private final SessionManager sessionManager;
    private final TopicRouter router;
    private final RetainedStore retainedStore;

    private volatile Session session;

    public MqttServerHandler(BrokerConfig config, SessionManager sessionManager,
                             TopicRouter router, RetainedStore retainedStore) {
        this.config = config;
        this.sessionManager = sessionManager;
        this.router = router;
        this.retainedStore = retainedStore;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) {
        if (msg.decoderResult() != null && msg.decoderResult().isFailure()) {
            log.warn("报文解码失败: {}", msg.decoderResult().cause());
            ctx.close();
            return;
        }
        switch (msg.fixedHeader().messageType()) {
            case CONNECT -> onConnect(ctx, (MqttConnectMessage) msg);
            case PUBLISH -> onPublish(ctx, (MqttPublishMessage) msg);
            case PUBACK -> onPubAck((MqttPubAckMessage) msg);
            case SUBSCRIBE -> onSubscribe(ctx, (MqttSubscribeMessage) msg);
            case UNSUBSCRIBE -> onUnsubscribe(ctx, (MqttUnsubscribeMessage) msg);
            case PINGREQ -> ctx.writeAndFlush(new MqttMessage(
                    new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0)));
            case DISCONNECT -> {
                if (session != null) {
                    session.markGraceful();
                }
                ctx.close();
            }
            default -> {
                log.warn("不支持的报文类型 {}", msg.fixedHeader().messageType());
                ctx.close();
            }
        }
    }

    private void onConnect(ChannelHandlerContext ctx, MqttConnectMessage msg) {
        if (session != null) {
            ctx.close();
            return;
        }
        var vh = msg.variableHeader();
        if (!"MQTT".equals(vh.name()) || vh.version() != 4) {
            refuse(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION);
            return;
        }
        if (!msg.variableHeader().isCleanSession()) {
            refuse(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED);
            return;
        }
        if (msg.payload().clientIdentifier() == null || msg.payload().clientIdentifier().isEmpty()) {
            refuse(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED);
            return;
        }
        Session created = new Session(msg.payload().clientIdentifier(), ctx.channel());
        if (vh.isWillFlag()) {
            MqttQoS willQos = vh.willQos() == 0 ? MqttQoS.AT_MOST_ONCE
                    : vh.willQos() == 1 ? MqttQoS.AT_LEAST_ONCE : null;
            if (willQos == null || !Topics.isValidPublishTopic(msg.payload().willTopic())) {
                refuse(ctx, MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION);
                return;
            }
            created.will(new Session.Will(msg.payload().willTopic(),
                    msg.payload().willMessageInBytes(), willQos, vh.isWillRetain()));
        }
        session = created;
        sessionManager.register(created);
        int keepAlive = vh.keepAliveTimeSeconds();
        if (keepAlive > 0) {
            int timeout = (int) Math.ceil(keepAlive * 1.5);
            ctx.pipeline().addBefore(ctx.name(), IDLE_HANDLER,
                    new IdleStateHandler(0, 0, timeout));
        }
        ctx.writeAndFlush(new MqttConnAckMessage(
                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false)));
        log.info("clientId={} 已连接 keepAlive={}s", created.clientId(), keepAlive);
    }

    private void refuse(ChannelHandlerContext ctx, MqttConnectReturnCode code) {
        ctx.writeAndFlush(new MqttConnAckMessage(
                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttConnAckVariableHeader(code, false))).addListener(f -> ctx.close());
    }

    private void onPublish(ChannelHandlerContext ctx, MqttPublishMessage msg) {
        if (session == null) {
            ctx.close();
            return;
        }
        String topic = msg.variableHeader().topicName();
        MqttQoS qos = msg.fixedHeader().qosLevel();
        if (!Topics.isValidPublishTopic(topic) || qos == MqttQoS.EXACTLY_ONCE) {
            log.warn("非法发布 topic={} qos={}，断开", topic, qos);
            ctx.close();
            return;
        }
        byte[] payload = ByteBufUtil.getBytes(msg.payload());
        if (msg.fixedHeader().isRetain()) {
            retainedStore.update(topic, payload, qos);
        }
        router.route(topic, payload, qos, false);
        if (qos == MqttQoS.AT_LEAST_ONCE) {
            ctx.writeAndFlush(new MqttPubAckMessage(
                    new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                    MqttMessageIdVariableHeader.from(msg.variableHeader().packetId())));
        }
    }

    private void onPubAck(MqttPubAckMessage msg) {
        if (session != null) {
            session.ackPacket(msg.variableHeader().messageId());
        }
    }

    private void onSubscribe(ChannelHandlerContext ctx, MqttSubscribeMessage msg) {
        if (session == null) {
            ctx.close();
            return;
        }
        List<Integer> granted = new ArrayList<>();
        List<MqttTopicSubscription> accepted = new ArrayList<>();
        for (MqttTopicSubscription sub : msg.payload().topicSubscriptions()) {
            MqttQoS q = sub.qualityOfService();
            if (q == MqttQoS.EXACTLY_ONCE || !Topics.isValidFilter(sub.topicName())
                    || session.subscriptions().size() >= config.maxSubscriptionsPerClient()
                            && !session.subscriptions().containsKey(sub.topicName())) {
                granted.add(0x80);
                continue;
            }
            session.subscriptions().put(sub.topicName(), q); // 同过滤器重订阅替换 QoS
            granted.add(q.value());
            accepted.add(sub);
        }
        ctx.writeAndFlush(new MqttSubAckMessage(
                new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(msg.variableHeader().messageId()),
                new MqttSubAckPayload(granted)));
        for (MqttTopicSubscription sub : accepted) {
            for (RetainedStore.Retained r : retainedStore.match(sub.topicName())) {
                MqttQoS outQos = MqttQoS.valueOf(
                        Math.min(r.qos().value(), sub.qualityOfService().value()));
                router.deliver(session, r.topic(), r.payload(), outQos, true);
            }
        }
    }

    private void onUnsubscribe(ChannelHandlerContext ctx, MqttUnsubscribeMessage msg) {
        if (session == null) {
            ctx.close();
            return;
        }
        for (String filter : msg.payload().topics()) {
            session.subscriptions().remove(filter);
        }
        ctx.writeAndFlush(new MqttUnsubAckMessage(
                new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(msg.variableHeader().messageId())));
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent e && e.state() == IdleState.ALL_IDLE) {
            log.warn("clientId={} 超过 KeepAlive 1.5 倍无报文，判定失联",
                    session != null ? session.clientId() : "?");
            ctx.close();
            return;
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Session s = session;
        if (s != null) {
            if (!s.graceful() && !s.takenOver() && s.will() != null) {
                Session.Will will = s.will();
                log.info("发布遗嘱 clientId={} topic={}", s.clientId(), will.topic());
                if (will.retain()) {
                    retainedStore.update(will.topic(), will.payload(), will.qos());
                }
                router.route(will.topic(), will.payload(), will.qos(), false);
            }
            sessionManager.remove(s);
            session = null;
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("连接异常: {}", cause.toString());
        ctx.close();
    }
}
