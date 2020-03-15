package com.github.zzt93.syncer.producer.input.mongo;

import com.github.zzt93.syncer.config.common.MongoConnection;
import com.github.zzt93.syncer.config.producer.ProducerMaster;
import com.github.zzt93.syncer.producer.register.ConsumerRegistry;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class MongoMasterConnectorFactory {

  private static final String LOCAL = "local";
  private static final int DEPLOYMENT_CHANGE_STREAM_VERSION = 4;
  private final Logger logger = LoggerFactory.getLogger(MongoMasterConnectorFactory.class);

  private final int mainVersion;
  private final ConsumerRegistry registry;
  private final MongoConnection connection;


  public MongoMasterConnectorFactory(MongoConnection connection, ConsumerRegistry registry) {
    this.registry = registry;
    this.connection = connection;
    MongoClient client = new MongoClient(new MongoClientURI(connection.toConnectionUrl(null)));
    mainVersion = getMainVersion(client);
  }


  private int getMainVersion(MongoClient client) {
    String version = client.getDatabase(LOCAL).runCommand(new BsonDocument("buildinfo", new BsonString("")))
        .get("version")
        .toString();
    return Integer.parseInt(version.split("\\.")[0]);
  }

  public MongoConnectorBase getMongoConnectorByServerVersion(ProducerMaster producerMaster) {
    if (mainVersion >= DEPLOYMENT_CHANGE_STREAM_VERSION) { // https://docs.mongodb.com/manual/changeStreams/#watch-collection-database-deployment
      return new MongoV4MasterConnector(connection, registry, producerMaster.mongoV4Option());
    }
    return new MongoMasterConnector(connection, registry);
  }


}
