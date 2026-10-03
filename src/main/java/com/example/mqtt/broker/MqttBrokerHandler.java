package com.example.mqtt.broker;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/** 协议接入层：校验报文并分派到 Broker；连接生命周期在此汇聚。 */
public final class MqttBrokerHandler extends SimpleChannelInboundHandler<MqttMessage> {
    private static final Logger log = LoggerFactory.getLogger(MqttBrokerHandler.class);

    private final Broker broker;
    private final int maxPendingOutbound;
    private Session session;

    public MqttBrokerHandler(Broker broker, int maxPendingOutbound) {
        this.broker = broker;
        this.maxPendingOutbound = maxPendingOutbound;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) {
        if (session != null) {
            session.touch();
        }
        switch (msg.fixedHeader().messageType()) {
            case CONNECT -> onConnect(ctx, (MqttConnectMessage) msg);
            case SUBSCRIBE -> onSubscribe(ctx, (MqttSubscribeMessage) msg);
            case UNSUBSCRIBE -> onUnsubscribe(ctx, (MqttUnsubscribeMessage) msg);
            case PUBLISH -> onPublish(ctx, (MqttPublishMessage) msg);
            case PUBACK -> onPubAck((MqttPubAckMessage) msg);
            case PINGREQ -> ctx.writeAndFlush(pingResp());
            case DISCONNECT -> {
                if (session != null) {
                    session.markGraceful();
                }
                ctx.close();
            }
            default -> {
                log.warn("unsupported packet type {}", msg.fixedHeader().messageType());
                ctx.close();
            }
        }
    }

    private void onConnect(ChannelHandlerContext ctx, MqttConnectMessage msg) {
        if (session != null) {
            ctx.close();
            return;
        }
        // 仅接受 MQTT 3.1.1（协议名 MQTT，级别 4）
        if (!"MQTT".equals(msg.variableHeader().name()) || msg.variableHeader().version() != 4) {
            ctx.writeAndFlush(connAck(MqttConnAckReturnCodeRef.UNACCEPTABLE_PROTOCOL_VERSION))
                    .addListener(f -> ctx.close());
            return;
        }
        // 仅支持 CleanSession=1
        if (!msg.variableHeader().isCleanSession()) {
            ctx.writeAndFlush(connAck(MqttConnAckReturnCodeRef.IDENTIFIER_REJECTED))
                    .addListener(f -> ctx.close());
            return;
        }
        String clientId = msg.payload().clientIdentifier();
        if (clientId == null || clientId.isEmpty()) {
            ctx.writeAndFlush(connAck(MqttConnAckReturnCodeRef.IDENTIFIER_REJECTED))
                    .addListener(f -> ctx.close());
            return;
        }
        Session s = new Session(clientId, ctx.channel(), maxPendingOutbound);
        s.setKeepAliveSeconds(msg.variableHeader().keepAliveTimeSeconds());
        if (msg.variableHeader().isWillFlag()) {
            int willQos = msg.variableHeader().willQos();
            if (willQos > 1 || !Topics.isValidPublishTopic(msg.payload().willTopic())) {
                ctx.close();
                return;
            }
            s.setWill(new Session.WillMessage(
                    msg.payload().willTopic(),
                    msg.payload().willMessageInBytes(),
                    willQos,
                    msg.variableHeader().isWillRetain()));
        }
        this.session = s;
        broker.attach(s);
        ctx.writeAndFlush(connAck(MqttConnAckReturnCodeRef.ACCEPTED));
        log.info("client {} connected, keepAlive={}s", clientId, msg.variableHeader().keepAliveTimeSeconds());
    }

    private void onSubscribe(ChannelHandlerContext ctx, MqttSubscribeMessage msg) {
        if (!requireSession(ctx)) {
            return;
        }
        List<Integer> granted = new ArrayList<>();
        List<String> replayFilters = new ArrayList<>();
        for (MqttTopicSubscription sub : msg.payload().topicSubscriptions()) {
            int qos = sub.qualityOfService().value();
            if (qos > 1 || !Topics.isValidFilter(sub.topicName())) {
                granted.add(0x80); // 失败
                continue;
            }
            session.subscribe(sub.topicName(), qos); // 重订阅替换 QoS
            granted.add(qos);
            replayFilters.add(sub.topicName());
        }
        MqttSubAckMessage ack = new MqttSubAckMessage(
                new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                msg.variableHeader(),
                new MqttSubAckPayload(granted.stream().mapToInt(Integer::intValue).toArray()));
        session.write(ack);
        for (String filter : replayFilters) {
            broker.replayRetained(session, filter);
        }
    }

    private void onUnsubscribe(ChannelHandlerContext ctx, MqttUnsubscribeMessage msg) {
        if (!requireSession(ctx)) {
            return;
        }
        for (String filter : msg.payload().topics()) {
            session.unsubscribe(filter); // 退订后停止投递
        }
        ctx.writeAndFlush(new MqttUnsubAckMessage(
                new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                msg.variableHeader()));
    }

    private void onPublish(ChannelHandlerContext ctx, MqttPublishMessage msg) {
        if (!requireSession(ctx)) {
            return;
        }
        MqttQoS qos = msg.fixedHeader().qosLevel();
        if (qos == MqttQoS.EXACTLY_ONCE) {
            log.warn("reject QoS2 publish from {}", session.clientId());
            ctx.close(); // 协议不支持 QoS2
            return;
        }
        String topic = msg.variableHeader().topicName();
        if (!Topics.isValidPublishTopic(topic)) {
            log.warn("invalid publish topic '{}' from {}", topic, session.clientId());
            ctx.close();
            return;
        }
        byte[] payload = new byte[msg.payload().readableBytes()];
        msg.payload().readBytes(payload);
        broker.publish(session, topic, payload, qos.value(), msg.fixedHeader().isRetain());
        if (qos == MqttQoS.AT_LEAST_ONCE) {
            ctx.writeAndFlush(new MqttPubAckMessage(
                    new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                    io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader.from(msg.variableHeader().packetId())));
        }
    }

    private void onPubAck(MqttPubAckMessage msg) {
        if (session != null) {
            session.releaseInflight(msg.variableHeader().messageId());
        }
    }

    private boolean requireSession(ChannelHandlerContext ctx) {
        if (session == null) {
            ctx.close();
            return false;
        }
        return true;
    }

    private static MqttMessage pingResp() {
        return new MqttMessage(new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0));
    }

    private static MqttConnAckMessage connAck(MqttConnectReturnCode code) {
        return new MqttConnAckMessage(
                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttConnAckVariableHeader(code, false));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (session != null) {
            broker.detach(session);
            session = null;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.debug("connection error: {}", cause.toString());
        ctx.close();
    }

    /** 常量别名，避免导入冲突。 */
    private static final class MqttConnAckReturnCodeRef {
        static final MqttConnectReturnCode ACCEPTED = MqttConnectReturnCode.CONNECTION_ACCEPTED;
        static final MqttConnectReturnCode UNACCEPTABLE_PROTOCOL_VERSION =
                MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION;
        static final MqttConnectReturnCode IDENTIFIER_REJECTED =
                MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED;
    }
}
