package com.example.mqtt.broker;

/** Broker 运行参数，仅内存态，可通过命令行/系统属性覆盖。 */
public final class BrokerConfig {
    public static final String HOST = "127.0.0.1";

    private final int port;
    private final int maxMessageSize;
    private final int maxPendingOutbound;

    public BrokerConfig(int port, int maxMessageSize, int maxPendingOutbound) {
        this.port = port;
        this.maxMessageSize = maxMessageSize;
        this.maxPendingOutbound = maxPendingOutbound;
    }

    public static BrokerConfig fromArgs(String[] args) {
        int port = Integer.parseInt(System.getProperty("mqtt.port", "1883"));
        int maxMsg = Integer.parseInt(System.getProperty("mqtt.maxMessageSize", "262144"));
        int maxPending = Integer.parseInt(System.getProperty("mqtt.maxPendingOutbound", "1024"));
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        return new BrokerConfig(port, maxMsg, maxPending);
    }

    public int port() {
        return port;
    }

    public int maxMessageSize() {
        return maxMessageSize;
    }

    public int maxPendingOutbound() {
        return maxPendingOutbound;
    }
}
