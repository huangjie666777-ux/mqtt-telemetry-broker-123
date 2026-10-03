package com.example.mqtt.broker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/** 每主题保留最新一条消息（载荷+QoS），仅存内存，断开不清理。 */
public final class RetainedStore {
    public record RetainedMessage(String topic, byte[] payload, int qos) {
    }

    private final Map<String, RetainedMessage> store = new ConcurrentHashMap<>();

    /** 零字节载荷清除旧值，否则覆盖保存。 */
    public void update(String topic, byte[] payload, int qos) {
        if (payload.length == 0) {
            store.remove(topic);
        } else {
            store.put(topic, new RetainedMessage(topic, payload.clone(), qos));
        }
    }

    /** 遍历与过滤器匹配的保留消息。 */
    public void forEachMatch(String filter, BiConsumer<String, RetainedMessage> consumer) {
        store.forEach((topic, msg) -> {
            if (Topics.matches(filter, topic)) {
                consumer.accept(topic, msg);
            }
        });
    }

    public int size() {
        return store.size();
    }
}
