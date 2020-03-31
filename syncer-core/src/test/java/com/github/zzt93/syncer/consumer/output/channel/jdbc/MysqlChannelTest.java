package com.github.zzt93.syncer.consumer.output.channel.jdbc;

import com.github.zzt93.syncer.config.common.MysqlConnection;
import com.google.gson.reflect.TypeToken;
import com.mysql.jdbc.Driver;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.net.UnknownHostException;
import java.sql.BatchUpdateException;
import java.util.Arrays;
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
    JdbcTemplate jdbcTemplate = getJdbcTemplate();

//    testSqlEscape(jdbcTemplate);
    testDiffError(jdbcTemplate);
  }

  private static JdbcTemplate getJdbcTemplate() throws UnknownHostException {
    MysqlConnection connection = new MysqlConnection();
    connection.setAddress("192.168.1.204");
    connection.setPort(3306);
    connection.setUser(System.getenv("MYSQL_USER"));
    connection.setPassword(System.getenv("MYSQL_PASS"));

    HikariConfig config = connection.toConfig();
    config.setDriverClassName(Driver.class.getName());
    config.setInitializationFailTimeout(-1);
    HikariDataSource hikariDataSource = new HikariDataSource(config);

    return new JdbcTemplate(hikariDataSource);
  }

  private static void testSqlEscape(JdbcTemplate jdbcTemplate) {
    // escape ' -> '', \ -> \\\\
    String sql = "insert into `test_0`.`types_bak` (`double`,`varchar`,`char`,`tinyint`,`id`,`text`,`decimal`,`bigint`,`timestamp`) values (0.14801252,'n','t2klRNvqH$y;X#zb-#o6[|5[.',100,810773732300543114," +
        "'rt0s{4tioX^I39@nptPw-) ySA_''l]j]iro#}N].k8Zst2)(LF%1=JM3MvY=<T1&`[~(<8b}6;y)Zct0%`hsw`.h.POg@N9>\\\\'')6KZY#8rpe4Iu;wBL-zW9*Ef.<kr)3jH{%&AK~a]'" +
        ",614224.31,730230726375786120,'2027-05-27 21:50:17.0')";
    jdbcTemplate.execute(sql);
  }

  private static void testDiffError(JdbcTemplate jdbcTemplate) {
    String[] sqls = {
        "delete from test_0.types_bak where id = 2125",
        "delete from test_0.types_bak where id = 2122",
        "insert into `test_0`.`types_bak` (`double`,`varchar`,`char`,`tinyint`,`id`,`text`,`decimal`,`bigint`,`timestamp`) values (0.6055158,'D5v','k',26,2125,'/>$Kf',19265911.19,1366022492355224397,'2017-12-01 22:30:24.0')",
        "insert into `test_0`.`types_bak` (`double`,`varchar`,`char`,`tinyint`,`id`,`text`,`decimal`,`bigint`,`timestamp`) values (0.6055158,'D5v','k',26,2125,'/>$Kf',19265911.19,1366022492355224397,'2017-12-01 22:30:24.0')",
        "insert into `test_0`.`types_bak` (`double`,`varchar`,`char`,`tinyint`,`id`,`text`,`decimal`,`bigint`,`timestamp`) values (0.47148514,'v[e|','6P{N(hb=8C6!t5oAfLv2',161,2122,'Qria3&&V',19265911.19,3128612873388751949,'2005-06-07 08:46:12.0')",
        "insert into `test_0`.`not_exists` (`double`) VALUES (1)",
        };
    try {
      jdbcTemplate.batchUpdate(sqls);
    } catch (DataAccessException e) {
      int[] updateCounts = ((BatchUpdateException) e.getCause()).getUpdateCounts();
      System.out.println(Arrays.toString(updateCounts));
      e.printStackTrace();
    }
  }
}