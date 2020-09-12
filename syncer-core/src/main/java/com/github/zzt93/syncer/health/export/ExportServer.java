package com.github.zzt93.syncer.health.export;

import com.github.zzt93.syncer.common.network.NettyServer;
import com.github.zzt93.syncer.config.syncer.SyncerConfig;
import com.github.zzt93.syncer.health.Health;
import com.github.zzt93.syncer.health.SyncerHealth;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static io.netty.handler.codec.http.HttpHeaderNames.*;
import static io.netty.handler.codec.http.HttpHeaderValues.*;
import static io.netty.handler.codec.http.HttpVersion.*;

/**
 * @author zzt
 */
public class ExportServer {


  private static final int PORT = Integer.parseInt(System.getProperty(SyncerConfig.SERVER_PORT, SyncerConfig.DEFAULT_START));
  static final String HEALTH = "/health";

  public static void init(SyncerConfig syncerConfig) throws Exception {
    Map<String, BiConsumer<ChannelHandlerContext, HttpRequest>> mapping = new HashMap<>();
    mapping.put(HEALTH, (channelHandlerContext, request) -> {
      ExportResult result = SyncerHealth.export();
      String json = result.getJson();
      Health.HealthStatus overall = result.getOverall();

      FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1,
          overall == Health.HealthStatus.GREEN ? HttpResponseStatus.OK : HttpResponseStatus.INTERNAL_SERVER_ERROR,
          Unpooled.wrappedBuffer(json.getBytes()));
      response.headers().set(CONTENT_TYPE, TEXT_PLAIN);
      response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
      channelHandlerContext.write(response);
    });

    ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new LoggingHandler(LogLevel.INFO));
        p.addLast(new HttpServerCodec());
        // add body support if we need in future
        // p.addLast(new HttpObjectAggregator(Short.MAX_VALUE));
        // add dispatch handler
        p.addLast(new DispatchHandler(mapping));
      }
    };

    // choose port logic
    int port = PORT;
    Integer cmdPort = syncerConfig.getPort();
    if (cmdPort != null) {
      port = cmdPort;
    }

    NettyServer.startAndSync(initializer, port);
  }

}
