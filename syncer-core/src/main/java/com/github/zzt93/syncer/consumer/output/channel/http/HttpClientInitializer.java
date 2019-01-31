package com.github.zzt93.syncer.consumer.output.channel.http;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;

/**
 * @author zzt
 */
public class HttpClientInitializer extends ChannelInitializer<SocketChannel> {

  @Override
  protected void initChannel(SocketChannel ch) {
    ChannelPipeline p = ch.pipeline();
    p.addLast(new HttpClientCodec());
    // Remove the following line if you don't want automatic content decompression.
    p.addLast(new HttpContentDecompressor());
  }
}
