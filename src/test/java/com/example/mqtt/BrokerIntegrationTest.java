package com.example.mqtt;

import com.example.mqtt.broker.BrokerConfig;
import com.example.mqtt.broker.MqttBrokerServer;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerIntegrationTest {
    private MqttBrokerServer server;
    private String uri;

    @BeforeEach
    void start() throws Exception {
        server = new MqttBrokerServer(new BrokerConfig(0, 65536, 256));
        server.start();
        uri = "tcp://127.0.0.1:" + server.boundPort();
    }

    @AfterEach
    void stop() {
        server.close();
    }

    private MqttClient client(String id) throws Exception {
        MqttClient c = new MqttClient(uri, id, new MemoryPersistence());
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setCleanSession(true);
        c.connect(opts);
        return c;
    }

    @Test
    void pubSubQos1AndUnsubscribe() throws Exception {
        List<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        MqttClient sub = client("sub1");
        sub.subscribe("sensors/+/temp", 1, (t, m) -> {
            received.add(t + "=" + new String(m.getPayload()) + " qos=" + m.getQos());
            latch.countDown();
        });

        MqttClient pub = client("pub1");
        pub.publish("sensors/room1/temp", "22.5".getBytes(), 1, false);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(List.of("sensors/room1/temp=22.5 qos=1"), received);

        sub.unsubscribe("sensors/+/temp");
        pub.publish("sensors/room1/temp", "23.0".getBytes(), 1, false);
        Thread.sleep(500);
        assertEquals(1, received.size(), "退订后不应再收到投递");

        pub.disconnect();
        sub.disconnect();
    }

    @Test
    void overlappingFiltersDeliverOnceWithMinQos() throws Exception {
        List<MqttMessage> received = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        MqttClient sub = client("sub2");
        sub.subscribe("sensors/#", 0, (t, m) -> {
            received.add(m);
            latch.countDown();
        });
        sub.subscribe("sensors/room1/#", 1);
        Thread.sleep(300);

        MqttClient pub = client("pub2");
        pub.publish("sensors/room1/hum", "40".getBytes(), 1, false);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        Thread.sleep(300);
        assertEquals(1, received.size(), "重叠过滤器只应投递一次");
        assertEquals(1, received.get(0).getQos(), "QoS=min(发布1, 订阅max(0,1))=1");

        pub.disconnect();
        sub.disconnect();
    }

    @Test
    void retainedReplayAndClear() throws Exception {
        MqttClient pub = client("pub3");
        pub.publish("dev/a/state", "ON".getBytes(), 1, true);

        CountDownLatch latch = new CountDownLatch(1);
        List<String> got = new CopyOnWriteArrayList<>();
        MqttClient sub = client("sub3");
        sub.subscribe("dev/#", 1, (t, m) -> {
            got.add(new String(m.getPayload()) + " retain=" + m.isRetained() + " qos=" + m.getQos());
            latch.countDown();
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS), "新订阅应收到保留消息");
        assertEquals(List.of("ON retain=true qos=1"), got);

        // 零字节保留发布清除旧值
        pub.publish("dev/a/state", new byte[0], 0, true);
        Thread.sleep(500);
        assertEquals(0, server.broker().retainedStore().size());

        pub.disconnect();
        sub.disconnect();
    }

    @Test
    void willPublishedOnAbnormalDisconnect() throws Exception {
        CountDownLatch willLatch = new CountDownLatch(1);
        List<String> willPayload = new CopyOnWriteArrayList<>();
        MqttClient monitor = client("monitor");
        monitor.subscribe("status/#", 1, (t, m) -> {
            willPayload.add(t + "=" + new String(m.getPayload()));
            willLatch.countDown();
        });

        // 原始 socket 客户端：CONNECT 带遗嘱，随后直接断开 TCP（不发 DISCONNECT）
        try (Socket raw = new Socket("127.0.0.1", server.boundPort())) {
            OutputStream out = raw.getOutputStream();
            out.write(connectWithWill("bad-sensor", "status/bad-sensor", "OFFLINE"));
            out.flush();
            byte[] connack = raw.getInputStream().readNBytes(4);
            assertEquals((byte) 0x20, connack[0]);
            assertEquals((byte) 0x00, connack[3]);
        } // close() 触发非正常断开

        assertTrue(willLatch.await(5, TimeUnit.SECONDS), "失联应触发遗嘱发布");
        assertEquals(List.of("status/bad-sensor=OFFLINE"), willPayload);
        monitor.disconnect();
    }

    @Test
    void sameClientIdTakeover() throws Exception {
        MqttClient first = client("dup");
        assertEquals(1, server.broker().registry().size());

        MqttClient second = client("dup");
        Thread.sleep(500);
        assertEquals(1, server.broker().registry().size(), "接管后仍只有一个会话");
        assertTrue(second.isConnected());

        // 新会话可正常收发
        CountDownLatch latch = new CountDownLatch(1);
        second.subscribe("t/x", 0, (t, m) -> latch.countDown());
        MqttClient pub = client("pub5");
        pub.publish("t/x", "hi".getBytes(), 0, false);
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        second.disconnect();
        pub.disconnect();
    }

    /** 手工构造 MQTT 3.1.1 CONNECT（CleanSession + Will flag，QoS0 遗嘱）。 */
    private static byte[] connectWithWill(String clientId, String willTopic, String willPayload) {
        byte[] cid = clientId.getBytes(StandardCharsets.UTF_8);
        byte[] wt = willTopic.getBytes(StandardCharsets.UTF_8);
        byte[] wp = willPayload.getBytes(StandardCharsets.UTF_8);
        // variable header: "MQTT" + level4 + flags(0x06: clean+will) + keepalive 30
        int remaining = 10 + 2 + cid.length + 2 + wt.length + 2 + wp.length;
        byte[] pkt = new byte[2 + remaining];
        int i = 0;
        pkt[i++] = 0x10;
        pkt[i++] = (byte) remaining;
        pkt[i++] = 0; pkt[i++] = 4; pkt[i++] = 'M'; pkt[i++] = 'Q'; pkt[i++] = 'T'; pkt[i++] = 'T';
        pkt[i++] = 4;
        pkt[i++] = 0x06; // clean session + will flag, will qos0
        pkt[i++] = 0; pkt[i++] = 30;
        i = putStr(pkt, i, cid);
        i = putStr(pkt, i, wt);
        putStr(pkt, i, wp);
        return pkt;
    }

    private static int putStr(byte[] dst, int i, byte[] s) {
        dst[i++] = (byte) (s.length >> 8);
        dst[i++] = (byte) s.length;
        System.arraycopy(s, 0, dst, i, s.length);
        return i + s.length;
    }
}
