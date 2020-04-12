package com.github.zzt93.syncer.test;

import com.github.zzt93.syncer.config.common.ElasticsearchConnection;
import com.github.zzt93.syncer.config.common.MongoConnection;
import com.github.zzt93.syncer.config.common.MysqlConnection;
import com.github.zzt93.syncer.config.consumer.input.MasterSourceType;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import lombok.AllArgsConstructor;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;

import static org.junit.Assert.*;

/**
 * @author zzt
 */
public class CompareDetail {

  private static Random r = new Random();
  private static Map<String, String[]> dbs = new HashMap<>();

  static {
    dbs.put("test", new String[]{"news", "correctness", "types"});
    dbs.put("simple", new String[]{"simple_type"});
    dbs.put("discard", new String[]{"toDiscard"});
    dbs.put("copy", new String[]{"toCopy"});
  }

  private static double cmpRate(int num) {
    if (num <= 100) {
      return 0.5;
    }
    if (num <= 500) {
      return 0.3;
    }
    return 0.2;
  }

  private static Map<String, Object> esDetail(AbstractClient client, String index, String type, int id) {
    SearchResponse response = client.prepareSearch(index).setTypes(type)
        .setQuery(QueryBuilders.termQuery("_id", id))
        .execute().actionGet();
    SearchHits hits = response.getHits();
    if (hits.totalHits == 0) {
      return Collections.emptyMap();
    }
    return hits.getAt(0).getSource();
  }

  private static Map<String, Object> mysqlDetail(JdbcTemplate jdbcTemplate, String db, String table, int id) {
    return jdbcTemplate.queryForMap(String.format("select * from %s.%s where id = %d", db, table, id));
  }

  private static Map<String, Object> mongoDetail(MongoClient mongoClient, String db, String col, int id) {
    return mongoClient.getDatabase(db).getCollection(col).find(new BsonDocument("_id", new BsonInt64(id))).first();
  }

  @Test
  public void compare() throws Exception {

    int idMin = getId("idMin", 0);
    int idMax = getId("idMax", 100);
    double cmpRate = cmpRate(idMax - idMin);

    Function<Selector, Map<String, Object>> inputSupplier = getInput();
    Function<Selector, Map<String, Object>> outputSupplier = getOutput();

    for (Map.Entry<String, String[]> e : dbs.entrySet()) {
      String db = e.getKey();
      for (String table : e.getValue()) {
        Selector t = new Selector(db, table, idMin);
        Map<String, Object> inputRes = inputSupplier.apply(t);
        Map<String, Object> outputRes = outputSupplier.apply(t);
        int inputSize = inputRes.size();
        int outputSize = outputRes.size();

        for (int id = idMin; id < idMax; id++) {
          if (r.nextDouble() >= cmpRate) {
            continue;
          }
          t = new Selector(db, table, id);
          inputRes = inputSupplier.apply(t);
          outputRes = outputSupplier.apply(t);

          assertTrue(inputSize == inputRes.size() || inputRes.isEmpty());
          assertTrue(outputSize == outputRes.size() || outputRes.isEmpty());
          for (String s : outputRes.keySet()) {
            if (inputRes.containsKey(s)) {
              assertEquals(inputRes.get(s), outputRes.get(s));
            } else {
              fail();
            }
          }
        }
      }
    }
  }

  private int getId(String prop, int defaultVal) {
    String property = System.getProperty(prop);
    if (property==null){
      return defaultVal;
    }
    return Integer.parseInt(property);
  }

  private Function<Selector, Map<String, Object>> getOutput() throws Exception {
    String outputEnv = System.getProperty("output");
    if (outputEnv == null) {
      outputEnv = OutputType.es.name();
    }
    OutputType output = OutputType.valueOf(outputEnv);

    Function<Selector, Map<String, Object>> outputSupplier;
    switch (output) {
      case es:
        AbstractClient esClient = getAbstractClient();
        outputSupplier = (Selector s) -> esDetail(esClient, s.db + "*", s.table, s.id);
        break;
//      case es7:
//        AbstractClient esClient = getAbstractClient();
//        outputSupplier = (Selector s) -> esDetail(esClient, s.table + "*", s.table, s.id);
//        break;
      case mysql:
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        outputSupplier = (Selector s) -> mysqlDetail(jdbcTemplate, s.db, s.table, s.id);
        break;
      default:
        throw new UnsupportedOperationException();
    }
    return outputSupplier;
  }

  private Function<Selector, Map<String, Object>> getInput() throws UnknownHostException {
    String inputEnv = System.getProperty("input");
    if (inputEnv == null) {
      inputEnv = MasterSourceType.MySQL.name();
    }
    MasterSourceType input = MasterSourceType.valueOf(inputEnv);

    Function<Selector, Map<String, Object>> inputSupplier;
    switch (input) {
      case MySQL:
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        inputSupplier = (Selector s) -> mysqlDetail(jdbcTemplate, s.db, s.table, s.id);
        break;
      case Mongo:
        MongoClient client = getMongoClient();
        inputSupplier = (Selector s) -> mongoDetail(client, s.db, s.table, s.id);
        break;
      default:
        throw new UnsupportedOperationException();
    }
    return inputSupplier;
  }

  private AbstractClient getAbstractClient() throws Exception {
    ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection();
    elasticsearchConnection.setClusterName("test-cluster");
    elasticsearchConnection.setClusterNodes(Lists.newArrayList("localhost:49300"));
    return elasticsearchConnection.esClient();
  }

  private JdbcTemplate getJdbcTemplate() throws UnknownHostException {
    MysqlConnection connection = new MysqlConnection("localhost", 43306, "root", "root");
    return new JdbcTemplate(connection.dataSource());
  }

  private MongoClient getMongoClient() throws UnknownHostException {
    MongoConnection connection = new MongoConnection("localhost", 47017, "root", "root");
    return new MongoClient(new MongoClientURI(connection.toConnectionUrl(null)));
  }

  private enum OutputType {
    es, es7, mysql,
  }

  @AllArgsConstructor
  private static class Selector {
    private String db;
    private String table;
    private int id;
  }

}
