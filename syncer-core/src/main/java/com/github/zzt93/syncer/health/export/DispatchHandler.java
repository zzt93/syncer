package com.github.zzt93.syncer.health.export;

import com.github.zzt93.syncer.health.Health;
import com.github.zzt93.syncer.health.SyncerHealth;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.rtsp.RtspHeaders.Names.CONTENT_LENGTH;

/**
 * @author zzt
 */
public class DispatchHandler extends SimpleChannelInboundHandler<HttpRequest> {


  @Override
  protected void channelRead0(ChannelHandlerContext ctx, HttpRequest msg) throws Exception {
    switch (msg.uri()) {
      case "/health":
        ExportResult result = SyncerHealth.export();
        String json = result.getJson();
        Health.HealthStatus overall = result.getOverall();

        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1,
            overall == Health.HealthStatus.GREEN ? HttpResponseStatus.OK : HttpResponseStatus.INTERNAL_SERVER_ERROR,
            Unpooled.wrappedBuffer(json.getBytes()));
        response.headers().set(CONTENT_TYPE, "text/plain");
        response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
        ctx.write(response);
    }
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
