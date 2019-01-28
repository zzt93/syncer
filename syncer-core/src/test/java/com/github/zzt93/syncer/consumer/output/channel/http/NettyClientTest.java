package com.github.zzt93.syncer.consumer.output.channel.http;

import com.github.zzt93.syncer.common.network.NettyServer;
import com.github.zzt93.syncer.config.common.HttpConnection;
import com.github.zzt93.syncer.health.export.DispatchHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;

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
        response.headers().set(CONTENT_TYPE, "text/plain");
        channelHandlerContext.write(response);
      });

      ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) {
          ChannelPipeline p = ch.pipeline();
          p.addLast(new HttpServerCodec());
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
    boolean post = nettyClient.write(HttpMethod.POST, TEST_NETTY_CLIENT, "{'a':1}");
    boolean put = nettyClient.write(HttpMethod.PUT, TEST_NETTY_CLIENT, "{'b':'c'}");
    boolean delete = nettyClient.write(HttpMethod.DELETE, TEST_NETTY_CLIENT, "{'d':4}");
    Assert.assertTrue(post);
    Assert.assertTrue(put);
    Assert.assertTrue(delete);
  }
}