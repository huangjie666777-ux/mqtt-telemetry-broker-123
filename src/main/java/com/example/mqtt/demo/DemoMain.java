package com.example.mqtt.demo;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * 实际演示：传感器发布(QoS1) -> 监测程序订阅(通配符) -> 保留消息回放 -> 遗嘱。
 * 用法：先启动 broker，再运行 mvn -o exec:java -Dexec.mainClass=com.example.mqtt.demo.DemoMain
 */
public final class DemoMain {
    public static void main(String[] args) throws Exception {
        String uri = "tcp://127.0.0.1:" + (args.length > 0 ? args[0]
                : System.getProperty("mqtt.port", "1883"));

        // 1) 监测程序：通配符订阅
        MqttClient monitor = new MqttClient(uri, "monitor", new MemoryPersistence());
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setCleanSession(true);
        monitor.connect(opts);
        monitor.subscribe("sensors/+/temp", 1,
                (t, m) -> System.out.printf("[monitor] %s = %s (qos=%d, retain=%b)%n",
                        t, new String(m.getPayload()), m.getQos(), m.isRetained()));
        monitor.subscribe("status/#", 1,
                (t, m) -> System.out.printf("[monitor] WILL  %s = %s%n", t, new String(m.getPayload())));

        // 2) 传感器：QoS1 周期上报 + 一条保留消息
        MqttClient sensor = new MqttClient(uri, "sensor-room1", new MemoryPersistence());
        sensor.connect(opts);
        sensor.publish("sensors/room1/temp", "22.5".getBytes(), 1, false);
        sensor.publish("sensors/room1/temp", "23.1".getBytes(), 1, true); // 保留
        Thread.sleep(500);

        // 3) 新订阅者上线 -> 立即收到保留值（RETAIN=1）
        MqttClient late = new MqttClient(uri, "late-joiner", new MemoryPersistence());
        late.connect(opts);
        late.subscribe("sensors/#", 1,
                (t, m) -> System.out.printf("[late]    retained %s = %s (retain=%b)%n",
                        t, new String(m.getPayload()), m.isRetained()));
        Thread.sleep(500);

        // 4) 异常传感器：带遗嘱连接后直接断电（关 socket，不发 DISCONNECT）
        try (Socket raw = new Socket("127.0.0.1", Integer.parseInt(uri.substring(uri.lastIndexOf(':') + 1)))) {
            OutputStream out = raw.getOutputStream();
            out.write(connectWithWill("sensor-room2", "status/sensor-room2", "LOST"));
            out.flush();
            raw.getInputStream().readNBytes(4); // CONNACK
            System.out.println("[demo]    sensor-room2 异常掉线（未发 DISCONNECT）");
        }
        Thread.sleep(1000);

        sensor.disconnect();
        monitor.disconnect();
        late.disconnect();
        System.out.println("[demo]    done");
    }

    private static byte[] connectWithWill(String clientId, String willTopic, String willPayload) {
        byte[] cid = clientId.getBytes(StandardCharsets.UTF_8);
        byte[] wt = willTopic.getBytes(StandardCharsets.UTF_8);
        byte[] wp = willPayload.getBytes(StandardCharsets.UTF_8);
        int remaining = 10 + 2 + cid.length + 2 + wt.length + 2 + wp.length;
        byte[] pkt = new byte[2 + remaining];
        int i = 0;
        pkt[i++] = 0x10;
        pkt[i++] = (byte) remaining;
        pkt[i++] = 0; pkt[i++] = 4; pkt[i++] = 'M'; pkt[i++] = 'Q'; pkt[i++] = 'T'; pkt[i++] = 'T';
        pkt[i++] = 4;
        pkt[i++] = 0x06;
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
