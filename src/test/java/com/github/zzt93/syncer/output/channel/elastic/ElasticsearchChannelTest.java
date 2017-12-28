package com.github.zzt93.syncer.output.channel.elastic;

import static java.util.Collections.emptyMap;

import java.io.IOException;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.Test;

/**
 * @author zzt
 */
public class ElasticsearchChannelTest {

  private static Script mockInlineScript(final String script) {
    return new Script(ScriptType.INLINE, "mock", script, emptyMap());
  }
  private final XContentParser createParser(XContent xContent, BytesReference data) throws IOException {
    return xContent.createParser(xContentRegistry(), data);
  }
  private NamedXContentRegistry xContentRegistry() {
    return new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
  }

  @Test
  public void toStr() throws Exception {
    UpdateRequest request = new UpdateRequest("test", "type1", "1")
        .script(mockInlineScript("ctx._source.body = \"foo\""));
    System.out.println(ElasticsearchChannel.toString(request));
    request = new UpdateRequest("test", "type1", "1").fromXContent(
        createParser(JsonXContent.jsonXContent, new BytesArray("{\"doc\": {\"body\": \"bar\"}}")));
    System.out.println(ElasticsearchChannel.toString(request));
  }
}