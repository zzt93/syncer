package com.github.zzt93.syncer.health.export;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Map;
import java.util.function.BiConsumer;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderValues.TEXT_PLAIN;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;

/**
 * @author zzt
 */
public class DispatchHandler extends SimpleChannelInboundHandler<HttpRequest> {

  private static final BiConsumer<ChannelHandlerContext, HttpRequest> defaultMapping = (channelHandlerContext, request) -> {
    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.NOT_FOUND, Unpooled.wrappedBuffer("No such endpoint\n".getBytes()));
    response.headers().set(CONTENT_TYPE, TEXT_PLAIN);
    response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
    channelHandlerContext.write(response);
  };
  private final Map<String, BiConsumer<ChannelHandlerContext, HttpRequest>> mapping;

  public DispatchHandler(Map<String, BiConsumer<ChannelHandlerContext, HttpRequest>> mapping) {
    this.mapping = mapping;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, HttpRequest request) throws Exception {
    mapping.getOrDefault(getEndpoint(request), defaultMapping).accept(ctx, request);
  }

  private String getEndpoint(HttpRequest request) {
    String s = request.uri();
    if (s.endsWith("/")) {
      return s.substring(0, s.length() - 1);
    }
    return s;
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    cause.printStackTrace();
    ctx.close();
  }
}
