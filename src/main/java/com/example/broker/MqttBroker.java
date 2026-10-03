package com.example.broker;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 代理入口：仅绑定 127.0.0.1，装配 Netty MQTT 编解码与业务处理器。 */
public final class MqttBroker {

    private static final Logger log = LoggerFactory.getLogger(MqttBroker.class);

    public static void main(String[] args) throws Exception {
        BrokerConfig config = BrokerConfig.fromArgs(args);
        SessionManager sessionManager = new SessionManager();
        RetainedStore retainedStore = new RetainedStore();
        TopicRouter router = new TopicRouter(sessionManager, config);

        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
                            new WriteBufferWaterMark(64 * 1024, 256 * 1024))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new MqttDecoder(config.maxMessageBytes()));
                            ch.pipeline().addLast(MqttEncoder.INSTANCE);
                            ch.pipeline().addLast(new MqttServerHandler(
                                    config, sessionManager, router, retainedStore));
                        }
                    });
            Channel server = bootstrap.bind(config.host(), config.port()).sync().channel();
            log.info("MQTT 3.1.1 遥测代理已启动，监听 {}:{}", config.host(), config.port());
            server.closeFuture().sync();
        } finally {
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }
    }
}
