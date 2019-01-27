package com.github.zzt93.syncer.health.export;

import com.github.zzt93.syncer.common.network.NettyServer;
import com.github.zzt93.syncer.common.util.ArgUtil;
import com.github.zzt93.syncer.health.Health;
import com.github.zzt93.syncer.health.SyncerHealth;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.rtsp.RtspHeaders.Names.CONTENT_LENGTH;

/**
 * @author zzt
 */
public class ExportServer {


  private static final int PORT = Integer.parseInt(System.getProperty("port", "40000"));

  public static void init(String[] args) throws Exception {
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

    ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new LoggingHandler(LogLevel.INFO));
        p.addLast(new HttpServerCodec());
        // add dispatch handler
        p.addLast(new DispatchHandler(mapping));
      }
    };

    // choose port logic
    int port = PORT;
    HashMap<String, String> kvMap = ArgUtil.toMap(args);
    String cmdPort = kvMap.get("port");
    if (cmdPort != null) {
      port = Integer.parseUnsignedInt(cmdPort);
    }

    NettyServer.startAndSync(initializer, port);
  }

}
