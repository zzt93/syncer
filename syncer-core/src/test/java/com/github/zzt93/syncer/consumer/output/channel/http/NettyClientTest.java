package com.github.zzt93.syncer.consumer.output.channel.http;

import com.github.zzt93.syncer.common.network.NettyServer;
import com.github.zzt93.syncer.config.common.HttpConnection;
import com.github.zzt93.syncer.health.Health;
import com.github.zzt93.syncer.health.SyncerHealth;
import com.github.zzt93.syncer.health.export.DispatchHandler;
import com.github.zzt93.syncer.health.export.ExportResult;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.rtsp.RtspHeaders.Names.CONTENT_LENGTH;

/**
 * @author zzt
 */
public class NettyClientTest {

  private static final int PORT = 50000;
  private NettyHttpClient nettyClient;

  @Before
  public void setUp() throws Exception {
    HttpConnection connection = new HttpConnection();
    connection.setAddress("localhost");
    connection.setPort(PORT);
    connection.setPath("/test/netty/client");
    nettyClient = new NettyHttpClient(connection, new HttpClientInitializer());

    new Thread(()->{
      Map<String, BiConsumer<ChannelHandlerContext, HttpRequest>> mapping = new HashMap<>();
      mapping.put("/health", (channelHandlerContext, request) -> {
        ExportResult result = SyncerHealth.export();
        String json = result.getJson();
        Health.HealthStatus overall = result.getOverall();

        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1,
            overall == Health.HealthStatus.GREEN ? HttpResponseStatus.OK : HttpResponseStatus.INTERNAL_SERVER_ERROR,
            Unpooled.wrappedBuffer(json.getBytes()));
        response.headers().set(CONTENT_TYPE, "text/plain");
        response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
        channelHandlerContext.write(response);
      });
      mapping.put("/test/netty/client", (channelHandlerContext, request) -> {
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
    }).start();
  }

  @Test
  public void write() throws InterruptedException {
    boolean post = nettyClient.write(HttpMethod.POST, "", "{}");
    boolean put = nettyClient.write(HttpMethod.PUT, "", "{}");
    boolean delete = nettyClient.write(HttpMethod.DELETE, "", "{}");
  }
}