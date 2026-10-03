package com.example.broker;

/** 代理运行参数：仅监听本机回环地址，端口与容量限制可配置。 */
public record BrokerConfig(
        String host,
        int port,
        int maxMessageBytes,
        int maxInflightPerClient,
        int maxSubscriptionsPerClient) {

    public static BrokerConfig fromArgs(String[] args) {
        int port = 1883;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        return new BrokerConfig("127.0.0.1", port, 256 * 1024, 64, 128);
    }
}
