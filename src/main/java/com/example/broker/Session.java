package com.example.broker;

import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** 单客户端会话：连接、订阅表、出站包 ID 与在途消息、遗嘱。仅内存态。 */
public final class Session {

    private final String clientId;
    private final Channel channel;
    private final Map<String, MqttQoS> subscriptions = new ConcurrentHashMap<>();
    private final Set<Integer> inflightPacketIds = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean disconnectedGracefully = new AtomicBoolean();
    private final AtomicBoolean takenOver = new AtomicBoolean();

    private volatile Will will;
    private volatile int nextPacketId = 1;

    public record Will(String topic, byte[] payload, MqttQoS qos, boolean retain) {
    }

    public Session(String clientId, Channel channel) {
        this.clientId = clientId;
        this.channel = channel;
    }

    public String clientId() {
        return clientId;
    }

    public Channel channel() {
        return channel;
    }

    public Map<String, MqttQoS> subscriptions() {
        return subscriptions;
    }

    public Will will() {
        return will;
    }

    public void will(Will will) {
        this.will = will;
    }

    public void markGraceful() {
        disconnectedGracefully.set(true);
    }

    public boolean graceful() {
        return disconnectedGracefully.get();
    }

    public void markTakenOver() {
        takenOver.set(true);
    }

    public boolean takenOver() {
        return takenOver.get();
    }

    /** 分配未占用的出站包 ID（1..65535 循环，跳过在途项）。 */
    public synchronized int nextPacketId() {
        for (int i = 0; i < 65535; i++) {
            int id = nextPacketId;
            nextPacketId = nextPacketId % 65535 + 1;
            if (inflightPacketIds.add(id)) {
                return id;
            }
        }
        return -1;
    }

    public boolean ackPacket(int packetId) {
        return inflightPacketIds.remove(packetId);
    }

    public int inflightCount() {
        return inflightPacketIds.size();
    }
}
