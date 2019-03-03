package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.zzt93.syncer.config.common.MysqlConnection;
import com.google.gson.reflect.TypeToken;
import com.mysql.jdbc.Driver;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.net.UnknownHostException;
import java.util.List;

/**
 * @author zzt
 */
public class MysqlChannelTest {


  @Test
  public void testGsonGeneric() throws Exception {
    TypeToken<List<String>> typeToken = new TypeToken<List<String>>() {
    };
    Assert.assertEquals(typeToken.getType().getTypeName(), "java.util.List<java.lang.String>");
    TypeToken<List<List<Integer>>> list = new TypeToken<List<List<Integer>>>() {
    };
    Assert.assertEquals(list.getType().getTypeName(), "java.util.List<java.util.List<java.lang.Integer>>");
  }

  public static void main(String[] args) throws UnknownHostException {
    MysqlConnection connection = new MysqlConnection();
    connection.setAddress("localhost");
    connection.setPort(43306);
    connection.setUser("root");
    connection.setPassword("root");

    HikariConfig config = connection.toConfig();
    config.setDriverClassName(Driver.class.getName());
    config.setInitializationFailTimeout(-1);
    HikariDataSource hikariDataSource = new HikariDataSource(config);

    JdbcTemplate jdbcTemplate = new JdbcTemplate(hikariDataSource);

    testSqlEscape(jdbcTemplate);
  }

  private static void testSqlEscape(JdbcTemplate jdbcTemplate) {
    // escape ' -> '', \ -> \\\\
    String sql = "insert into `test_0`.`types_bak` (`double`,`varchar`,`char`,`tinyint`,`id`,`text`,`decimal`,`bigint`,`timestamp`) values (0.14801252,'n','t2klRNvqH$y;X#zb-#o6[|5[.',100,810773732300543114," +
        "'rt0s{4tioX^I39@nptPw-) ySA_''l]j]iro#}N].k8Zst2)(LF%1=JM3MvY=<T1&`[~(<8b}6;y)Zct0%`hsw`.h.POg@N9>\\\\'')6KZY#8rpe4Iu;wBL-zW9*Ef.<kr)3jH{%&AK~a]'" +
        ",614224.31,730230726375786120,'2027-05-27 21:50:17.0')";
    jdbcTemplate.execute(sql);
  }
}