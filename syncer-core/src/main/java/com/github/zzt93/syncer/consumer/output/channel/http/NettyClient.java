package com.github.zzt93.syncer.consumer.output.channel.http;

import com.github.zzt93.syncer.config.common.HttpConnection;
import com.github.zzt93.syncer.config.common.InvalidConfigException;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;


/**
 * @author zzt
 */
public class NettyClient {

  private static final Logger logger = LoggerFactory.getLogger(NettyClient.class);
  private final HttpConnection connection;
  private final EventLoopGroup group = new NioEventLoopGroup();
  private Channel ch;

  NettyClient(HttpConnection connection) {
    this.connection = connection;
    try {
      Bootstrap b = new Bootstrap();
      b.group(group)
          .channel(NioSocketChannel.class)
          .handler(new HttpClientInitializer());

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

  public boolean write(HttpMethod method, String json) throws InterruptedException {
    // TODO 2019/1/26 lone-lived
    DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, "");
    request.headers().set(HttpHeaderNames.HOST, connection.getAddress());
    request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
    request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP);
    request.headers().add(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);

    ByteBuf byteBuf = Unpooled.wrappedBuffer(json.getBytes(StandardCharsets.UTF_8));
    request.headers().set(HttpHeaderNames.CONTENT_LENGTH, byteBuf.readableBytes());
    request.content().writeBytes(byteBuf);

    ch.writeAndFlush(request).sync();
    return false;
  }

  private class HttpClientInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel ch) {
      ChannelPipeline p = ch.pipeline();
      p.addLast(new HttpClientCodec());
      // Remove the following line if you don't want automatic content decompression.
      p.addLast(new HttpContentDecompressor());
      // Uncomment the following line if you don't want to handle HttpContents.
      p.addLast(new HttpObjectAggregator(1048576));
    }
  }
}
