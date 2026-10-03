package com.example.mqtt;

import com.example.mqtt.broker.RetainedStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RetainedStoreTest {
    @Test
    void keepsLatestAndClearsOnEmptyPayload() {
        RetainedStore store = new RetainedStore();
        store.update("a/b", "v1".getBytes(), 0);
        store.update("a/b", "v2".getBytes(), 1);
        List<String> payloads = new ArrayList<>();
        store.forEachMatch("a/#", (t, m) -> payloads.add(new String(m.payload()) + "@" + m.qos()));
        assertEquals(List.of("v2@1"), payloads);

        store.update("a/b", new byte[0], 0);
        assertEquals(0, store.size());
    }
}
