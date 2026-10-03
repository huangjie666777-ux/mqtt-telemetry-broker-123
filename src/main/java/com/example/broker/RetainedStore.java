package com.example.broker;

import io.netty.handler.codec.mqtt.MqttQoS;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** 保留消息存储：每主题仅存最新载荷与 QoS；零字节载荷清除旧值。 */
public final class RetainedStore {

    public record Retained(String topic, byte[] payload, MqttQoS qos) {
    }

    private final Map<String, Retained> retained = new ConcurrentHashMap<>();

    public void update(String topic, byte[] payload, MqttQoS qos) {
        if (payload.length == 0) {
            retained.remove(topic);
        } else {
            retained.put(topic, new Retained(topic, payload, qos));
        }
    }

    /** 返回与过滤器匹配的全部保留消息。 */
    public List<Retained> match(String filter) {
        List<Retained> out = new ArrayList<>();
        for (Retained r : retained.values()) {
            if (Topics.matches(filter, r.topic())) {
                out.add(r);
            }
        }
        return out;
    }
}
