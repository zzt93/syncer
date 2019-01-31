package com.github.zzt93.syncer.consumer.output.channel.http;

import com.github.zzt93.syncer.common.network.NettyClient;
import com.github.zzt93.syncer.config.common.Connection;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.http.*;

import java.nio.charset.StandardCharsets;


/**
 * @author zzt
 */
public class NettyHttpClient extends NettyClient {

  public NettyHttpClient(Connection connection, ChannelInitializer initializer) {
    super(connection, initializer);
  }

  public boolean write(HttpMethod method, String path, String body) throws InterruptedException {
    // TODO 2019/1/26 long-lived
    ByteBuf byteBuf = Unpooled.copiedBuffer(body.getBytes(StandardCharsets.UTF_8));
    DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path, byteBuf);
    request.headers().set(HttpHeaderNames.HOST, connection.getAddress());
    request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, "*/*");
    request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
    request.headers().add(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
    request.headers().set(HttpHeaderNames.CONTENT_LENGTH, byteBuf.readableBytes());

    ch.writeAndFlush(request).sync();
    // TODO 2019/1/31 return value
    return true;
  }

}
