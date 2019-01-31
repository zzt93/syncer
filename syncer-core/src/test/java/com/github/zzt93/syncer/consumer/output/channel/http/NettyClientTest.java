package com.github.zzt93.syncer.consumer.output.channel.http;

import com.github.zzt93.syncer.common.network.NettyServer;
import com.github.zzt93.syncer.config.common.HttpConnection;
import com.github.zzt93.syncer.health.export.DispatchHandler;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * @author zzt
 */
public class NettyClientTest {

  private static final int PORT = 50000;
  private static final String TEST_NETTY_CLIENT = "/test/netty/client";
  private NettyHttpClient nettyClient;
  private Thread thread;

  @Before
  public void setUp() throws Exception {
    thread = new Thread(() -> {
      Map<String, BiConsumer<ChannelHandlerContext, HttpRequest>> mapping = new HashMap<>();
      mapping.put(TEST_NETTY_CLIENT, (channelHandlerContext, request) -> {
        HttpResponseStatus status;
        if (request.method() == HttpMethod.POST) {
          status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
        } else if (request.method() == HttpMethod.PUT) {
          status = HttpResponseStatus.OK;
        } else {
          status = HttpResponseStatus.OK;
        }
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN);
        channelHandlerContext.write(response);
      });

      ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) {
          ChannelPipeline p = ch.pipeline();
          p.addLast(new HttpServerCodec());
          p.addLast(new HttpObjectAggregator(Short.MAX_VALUE));
          p.addLast(new DispatchHandler(mapping));
        }
      };
      try {
        NettyServer.startAndSync(initializer, PORT);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });
    thread.start();

    Thread.sleep(2000);

    HttpConnection connection = new HttpConnection();
    connection.setAddress("localhost");
    connection.setPort(PORT);
    connection.setPath(TEST_NETTY_CLIENT);
    nettyClient = new NettyHttpClient(connection, new HttpClientInitializer());
  }

  @Test
  public void write() throws InterruptedException {
    boolean post = nettyClient.write(HttpMethod.POST, TEST_NETTY_CLIENT, "{\"foo\":\"bar\"}");
    boolean put = nettyClient.write(HttpMethod.PUT, TEST_NETTY_CLIENT, "{\"b\":\"c\"}");
    boolean delete = nettyClient.write(HttpMethod.DELETE, TEST_NETTY_CLIENT, "{\"d\":4}");
    Assert.assertTrue(post);
    Assert.assertTrue(put);
    Assert.assertTrue(delete);
  }


  class InboundHandlerA extends SimpleChannelInboundHandler<FullHttpRequest> {
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      super.channelActive(ctx);
      System.out.println("Connected!");
    }

    // Please keep in mind that this method
    @Override
    public void channelRead0(ChannelHandlerContext ctx,
                             FullHttpRequest msg) throws Exception {

      System.out.println("Recieved request!");
      System.out.println("HTTP Method: " + msg.getMethod());
      System.out.println("HTTP Version: " + msg.getProtocolVersion());
      System.out.println("URI: " + msg.getUri());
      System.out.println("Headers: " + msg.headers());
      System.out.println("Trailing headers: " + msg.trailingHeaders());

      ByteBuf data = msg.content();
      System.out.println("POST/PUT length: " + data.readableBytes());
      System.out.println("POST/PUT as string: ");
      System.out.println("-- DATA --");
      System.out.println(data.toString(StandardCharsets.UTF_8));
      System.out.println("-- DATA END --");

      // Send response back so the browser won't timeout
      ByteBuf responseBytes = ctx.alloc().buffer();
      responseBytes.writeBytes("Hello World".getBytes());

      FullHttpResponse response = new DefaultFullHttpResponse(
          HttpVersion.HTTP_1_1, HttpResponseStatus.OK, responseBytes);
      response.headers().set(HttpHeaders.Names.CONTENT_TYPE,
          "text/plain");
      response.headers().set(HttpHeaders.Names.CONTENT_LENGTH,
          response.content().readableBytes());

      response.headers().set(HttpHeaders.Names.CONNECTION,
          HttpHeaders.Values.KEEP_ALIVE);
      ctx.write(response);
    }
  }
}