package com.github.zzt93.syncer.config.output;

import com.github.zzt93.syncer.output.OutputChannel;
import com.github.zzt93.syncer.util.FileUtil;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.commons.lang.StringUtils.*;

/**
 * @author zzt
 */
@Configuration
public class Elasticsearch implements OutputChannelConfig {

    private static final Logger logger = LoggerFactory.getLogger(Elasticsearch.class);
    private static final String COMMA = ",";
    private static final String COLON = ":";
    private String clusterName = "elasticsearch";
    private List<String> clusterNodes;
    private String user;
    private String passwordFile;
    private String index;
    private String type;
    private String documentId;
    private String password;

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public List<String> getClusterNodes() {
        return clusterNodes;
    }

    public void setClusterNodes(List<String> clusterNodes) {
        this.clusterNodes = clusterNodes;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPasswordFile() {
        return passwordFile;
    }

    public void setPasswordFile(String passwordFile) {
        this.passwordFile = passwordFile;
        try {
            this.password = FileUtil.readAll(passwordFile);
        } catch (IOException e) {
            logger.error("Invalid password file location of elasticsearch", e);
        }
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDocumentId() {
        return documentId;
    }

    public void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

    private String clusterNodesString() {
        return clusterNodes.stream().collect(Collectors.joining(","));
    }

    @Bean
    public TransportClient transportClient() throws Exception {
        PreBuiltXPackTransportClient client = new PreBuiltXPackTransportClient(settings());
        String clusterNodes = clusterNodesString();
        Assert.hasText(clusterNodes, "[Assertion failed] clusterNodes settings missing.");
        for (String clusterNode : split(clusterNodes, COMMA)) {
            String hostName = substringBeforeLast(clusterNode, COLON);
            String port = substringAfterLast(clusterNode, COLON);
            Assert.hasText(hostName, "[Assertion failed] missing host name in 'clusterNodes'");
            Assert.hasText(port, "[Assertion failed] missing port in 'clusterNodes'");
            logger.info("adding transport node : " + clusterNode);
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), Integer.valueOf(port)));
        }
        return client;
    }

    private Settings settings() {
        return Settings.builder()
                .put("cluster.name", getClusterName())
                .put("xpack.security.user", user + COLON + password)
//        .put("client.transport.sniff", clientTransportSniff)
//        .put("client.transport.ignore_cluster_name", clientIgnoreClusterName)
//        .put("client.transport.ping_timeout", clientPingTimeout)
//        .put("client.transport.nodes_sampler_interval", clientNodesSamplerInterval)
                .build();
    }

    @Override
    public OutputChannel build() {
        connect();
        return null;
    }

    private void connect() {

    }
}
