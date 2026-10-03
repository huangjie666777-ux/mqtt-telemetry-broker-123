package com.example.demo;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.nio.charset.StandardCharsets;

/**
 * 演示客户端：
 *   sub  <端口> <clientId> <过滤器>          订阅并打印收到的消息
 *   pub  <端口> <clientId> <主题> <载荷> [qos] [retain]  发布一条消息
 *   will <端口> <clientId> <遗嘱主题>        带遗嘱连接后异常退出（不发送 DISCONNECT）
 */
public final class DemoClient {

    public static void main(String[] args) throws Exception {
        String mode = args[0];
        int port = Integer.parseInt(args[1]);
        String clientId = args[2];
        String server = "tcp://127.0.0.1:" + port;
        switch (mode) {
            case "sub" -> subscribe(server, clientId, args[3]);
            case "pub" -> publish(server, clientId, args[3], args[4],
                    args.length > 5 ? Integer.parseInt(args[5]) : 0,
                    args.length > 6 && Boolean.parseBoolean(args[6]));
            case "will" -> willThenDie(server, clientId, args[3]);
            default -> throw new IllegalArgumentException("未知模式 " + mode);
        }
    }

    private static void subscribe(String server, String clientId, String filter) throws Exception {
        MqttClient client = new MqttClient(server, clientId);
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setCleanSession(true);
        opts.setKeepAliveInterval(10);
        client.setCallback(new MqttCallback() {
            public void connectionLost(Throwable cause) {
                System.out.println("[sub] 连接丢失: " + cause);
            }

            public void messageArrived(String topic, MqttMessage message) {
                System.out.printf("[sub] 收到 topic=%s qos=%d retained=%s payload=%s%n",
                        topic, message.getQos(), message.isRetained(),
                        new String(message.getPayload(), StandardCharsets.UTF_8));
            }

            public void deliveryComplete(IMqttDeliveryToken token) {
            }
        });
        client.connect(opts);
        client.subscribe(filter, 1);
        System.out.println("[sub] 已订阅 " + filter + "，等待消息…");
        Thread.currentThread().join();
    }

    private static void publish(String server, String clientId, String topic, String payload,
                                int qos, boolean retain) throws Exception {
        MqttClient client = new MqttClient(server, clientId);
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setCleanSession(true);
        client.connect(opts);
        MqttMessage msg = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
        msg.setQos(qos);
        msg.setRetained(retain);
        client.publish(topic, msg);
        client.disconnect();
        client.close();
        System.out.printf("[pub] 已发布 topic=%s qos=%d retain=%s payload=%s%n",
                topic, qos, retain, payload);
    }

    private static void willThenDie(String server, String clientId, String willTopic) throws Exception {
        MqttClient client = new MqttClient(server, clientId);
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setCleanSession(true);
        opts.setKeepAliveInterval(2);
        opts.setWill(willTopic, "sensor offline".getBytes(StandardCharsets.UTF_8), 1, false);
        client.connect(opts);
        System.out.println("[will] 已连接并注册遗嘱 topic=" + willTopic + "，现在模拟宕机…");
        Runtime.getRuntime().halt(1); // 不发送 DISCONNECT，直接退出进程
    }
}
