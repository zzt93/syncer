package com.github.zzt93.syncer.common.network;

import com.github.zzt93.syncer.config.common.Connection;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author zzt
 */
public class NettyClient {

  private static final Logger logger = LoggerFactory.getLogger(NettyClient.class);
  protected final Connection connection;
  protected final Channel ch;
  private final EventLoopGroup group = new NioEventLoopGroup();

  public NettyClient(Connection connection, ChannelInitializer initializer) {
    this.connection = connection;
    try {
      Bootstrap b = new Bootstrap();
      b.group(group)
          .channel(NioSocketChannel.class)
          .handler(initializer);

      ch = b.connect(connection.getAddress(), connection.getPort()).sync().channel();
    } catch (Throwable e) {
      logger.error("Fail to connect to {}", connection, e);
      throw new InvalidConfigException(e);
    }
  }

  public void close() {
    try {
      ch.closeFuture().sync();
    } catch (InterruptedException e) {
      logger.error("", e);
    } finally {
      // Shut down executor threads to exit.
      group.shutdownGracefully();
    }
  }

}
