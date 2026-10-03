package com.example.mqtt.broker;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Netty 服务端引导：仅绑定 127.0.0.1，MqttDecoder 处理半包粘包并限制报文大小。 */
public final class MqttBrokerServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MqttBrokerServer.class);

    private final BrokerConfig config;
    private final Broker broker = new Broker();
    private EventLoopGroup boss;
    private EventLoopGroup worker;
    private ScheduledExecutorService keepAliveSweeper;
    private Channel serverChannel;

    public MqttBrokerServer(BrokerConfig config) {
        this.config = config;
    }

    public Broker broker() {
        return broker;
    }

    public void start() throws InterruptedException {
        boss = new NioEventLoopGroup(1);
        worker = new NioEventLoopGroup();
        ServerBootstrap b = new ServerBootstrap()
                .group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.config().setWriteBufferWaterMark(new WriteBufferWaterMark(64 * 1024, 1024 * 1024));
                        ch.pipeline()
                                .addLast("decoder", new MqttDecoder(config.maxMessageSize()))
                                .addLast("encoder", MqttEncoder.INSTANCE)
                                .addLast("handler", new MqttBrokerHandler(broker, config.maxPendingOutbound()));
                    }
                })
                .childOption(ChannelOption.TCP_NODELAY, true);
        serverChannel = b.bind(BrokerConfig.HOST, config.port()).sync().channel();

        keepAliveSweeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "keepalive-sweeper");
            t.setDaemon(true);
            return t;
        });
        keepAliveSweeper.scheduleWithFixedDelay(this::sweepKeepAlive, 1, 1, TimeUnit.SECONDS);
        log.info("MQTT broker listening on {}:{}", BrokerConfig.HOST, config.port());
    }

    private void sweepKeepAlive() {
        long now = System.nanoTime();
        for (Session s : broker.registry().all()) {
            if (s.isExpired(now)) {
                log.warn("client {} keepalive expired (1.5x)", s.clientId());
                s.channel().close();
            }
        }
    }

    public int boundPort() {
        return ((java.net.InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    @Override
    public void close() {
        if (keepAliveSweeper != null) {
            keepAliveSweeper.shutdownNow();
        }
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
        }
        if (worker != null) {
            worker.shutdownGracefully().syncUninterruptibly();
        }
        if (boss != null) {
            boss.shutdownGracefully().syncUninterruptibly();
        }
    }
}
