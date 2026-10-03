package com.example.mqtt.broker;

/** 启动入口：java -jar ... [port]，默认 1883，仅监听 127.0.0.1。 */
public final class Main {
    public static void main(String[] args) throws Exception {
        BrokerConfig config = BrokerConfig.fromArgs(args);
        MqttBrokerServer server = new MqttBrokerServer(config);
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        Thread.currentThread().join();
    }
}
