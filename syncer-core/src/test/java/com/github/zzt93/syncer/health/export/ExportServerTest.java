package com.github.zzt93.syncer.health.export;

import com.github.zzt93.syncer.config.common.HttpConnection;
import com.github.zzt93.syncer.consumer.output.channel.http.HttpClientInitializer;
import com.github.zzt93.syncer.consumer.output.channel.http.NettyHttpClient;
import io.netty.handler.codec.http.HttpMethod;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;

public class ExportServerTest {

  private static final int PORT = 50000;
  private Thread thread;

  @Before
  public void setUp() throws Exception {
    thread = new Thread(() -> {
      try {
        ExportServer.init(new String[]{"--port=" + PORT});
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    thread.start();

    Thread.sleep(2000);

  }


  @Test
  public void init() throws UnknownHostException, InterruptedException {

    HttpConnection connection = new HttpConnection();
    connection.setAddress("localhost");
    connection.setPort(PORT);
    connection.setPath(ExportServer.HEALTH);
    NettyHttpClient nettyClient = new NettyHttpClient(connection, new HttpClientInitializer());
    nettyClient.write(HttpMethod.GET, ExportServer.HEALTH, "");

    System.out.println();
  }
}