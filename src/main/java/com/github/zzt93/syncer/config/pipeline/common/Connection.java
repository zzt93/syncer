package com.github.zzt93.syncer.config.pipeline.common;

import com.github.zzt93.syncer.common.util.FileUtil;
import com.github.zzt93.syncer.common.util.NetworkUtil;
import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

/**
 * @author zzt
 */
public class Connection implements Comparable<Connection> {

  private static final Logger logger = LoggerFactory.getLogger(Connection.class);

  private String address;
  private int port;
  private String user;
  private String passwordFile;
  private String password;
  private String identifier;
  private String ip;

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) throws UnknownHostException {
    this.address = address;
    ip = NetworkUtil.toIp(getAddress());
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
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
    } catch (Exception e) {
      logger
          .error("Fail to read password file from classpath, you may consider using absolute path",
              e);
    }
  }

  public String getPassword() {
    if (password == null) {
      throw new InvalidConfigException("No passwordFile/password set");
    }
    return password;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !(o instanceof Connection)) {
      return false;
    }

    Connection that = (Connection) o;

    return port == that.port && ip.equals(that.ip);
  }

  @Override
  public int hashCode() {
    int result = ip.hashCode();
    result = 31 * result + port;
    return result;
  }

  @Override
  public String toString() {
    return "Connection{" +
        "address='" + address + '\'' +
        ", port=" + port +
        ", user='" + user + '\'' +
        ", passwordFile='" + passwordFile + '\'' +
        '}';
  }

  public boolean valid() {
    return address != null && port > 0 && port < 65536;
  }

  public String initIdentifier() {
    identifier = ip + ":" + getPort();
    return identifier;
  }

  public String connectionIdentifier() {
    Assert.notNull(identifier, "[should invoke initIdentifier() first]");
    return identifier;
  }

  public String toConnectionUrl(String path) {
    return getAddress() + ":" + getPort();
  }

  @Override
  public int compareTo(Connection o) {
    int compare = ip.compareTo(o.ip);
    return compare != 0 ? compare : Integer.compare(port, o.port);
  }
}
