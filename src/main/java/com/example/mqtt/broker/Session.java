package com.example.mqtt.broker;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.mqtt.MqttPublishMessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** 单客户端会话：订阅表、出站在途 QoS1、包 ID 分配、写缓冲记账。 */
public final class Session {
    public record WillMessage(String topic, byte[] payload, int qos, boolean retain) {
    }

    private final String clientId;
    private final Channel channel;
    private final int maxPendingOutbound;

    private final Map<String, Integer> subscriptions = new ConcurrentHashMap<>();
    private final Map<Integer, MqttPublishMessage> inflightOut = new ConcurrentHashMap<>();
    private final AtomicInteger nextPacketId = new AtomicInteger(1);
    private final AtomicInteger pendingWrites = new AtomicInteger();

    private volatile WillMessage will;
    private volatile int keepAliveSeconds;
    private volatile long lastActivityNanos = System.nanoTime();
    private volatile boolean gracefulDisconnect;

    public Session(String clientId, Channel channel, int maxPendingOutbound) {
        this.clientId = clientId;
        this.channel = channel;
        this.maxPendingOutbound = maxPendingOutbound;
    }

    public String clientId() {
        return clientId;
    }

    public Channel channel() {
        return channel;
    }

    public void setWill(WillMessage w) {
        this.will = w;
    }

    public WillMessage will() {
        return will;
    }

    public void setKeepAliveSeconds(int s) {
        this.keepAliveSeconds = s;
    }

    public void touch() {
        lastActivityNanos = System.nanoTime();
    }

    /** 按 KeepAlive 的 1.5 倍判定失联。 */
    public boolean isExpired(long nowNanos) {
        if (keepAliveSeconds <= 0) {
            return false;
        }
        long limit = (long) (keepAliveSeconds * 1.5d * 1_000_000_000L);
        return nowNanos - lastActivityNanos > limit;
    }

    public void markGraceful() {
        gracefulDisconnect = true;
    }

    public boolean isGraceful() {
        return gracefulDisconnect;
    }

    /** 同过滤器重订阅替换 QoS。 */
    public void subscribe(String filter, int qos) {
        subscriptions.put(filter, qos);
    }

    public void unsubscribe(String filter) {
        subscriptions.remove(filter);
    }

    public Map<String, Integer> subscriptions() {
        return subscriptions;
    }

    /** 分配未被占用的出站包 ID（1..65535 循环，跳过在途项）。 */
    public synchronized int nextOutboundPacketId() {
        for (int tries = 0; tries < 65535; tries++) {
            int id = nextPacketId.getAndUpdate(v -> v >= 65535 ? 1 : v + 1);
            if (!inflightOut.containsKey(id)) {
                return id;
            }
        }
        throw new IllegalStateException("no free packet id");
    }

    public void addInflight(int packetId, MqttPublishMessage msg) {
        inflightOut.put(packetId, msg);
    }

    public void releaseInflight(int packetId) {
        inflightOut.remove(packetId);
    }

    public int inflightCount() {
        return inflightOut.size();
    }

    /** 写出并记账；超过待发送上限返回 false（调用方断开慢客户端）。 */
    public boolean write(Object msg) {
        int pending = pendingWrites.incrementAndGet();
        if (pending > maxPendingOutbound) {
            pendingWrites.decrementAndGet();
            return false;
        }
        channel.writeAndFlush(msg).addListener((ChannelFutureListener) f -> pendingWrites.decrementAndGet());
        return true;
    }

    public int pendingWrites() {
        return pendingWrites.get();
    }
}
