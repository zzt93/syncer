package com.github.zzt93.syncer.config.common;

import com.github.zzt93.syncer.common.util.FileUtil;
import com.github.zzt93.syncer.common.util.NetworkUtil;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.net.UnknownHostException;

/**
 * @author zzt
 */
public class Connection implements Comparable<Connection> {

  private static final Logger logger = LoggerFactory.getLogger(Connection.class);
  private static final String COMMON = ":";

  private String address;
  private int port;
  private String user;
  private String passwordFile;
  private String password;
  private volatile String identifier;
  private String ip;

  public Connection() {
  }

  public Connection(Connection connection) {
    address = connection.address;
    port = connection.port;
    user = connection.user;
    passwordFile = connection.passwordFile;
    password = connection.password;
    identifier = connection.identifier;
    ip = connection.ip;
  }

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
    Preconditions.checkNotNull(passwordFile);
    this.passwordFile = passwordFile;
    try {
      this.password = FileUtil.readLine(passwordFile).get(0);
    } catch (Exception e) {
      logger
          .error("Fail to read password file from classpath, you may consider using absolute path",
              e);
    }
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getPassword() {
    if (password == null) {
      throw new InvalidConfigException("No passwordFile/password set");
    }
    return password;
  }

  public boolean noPassword() {
    return StringUtils.isEmpty(password);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Connection)) {
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

  public String connectionIdentifier() {
    if (identifier == null) {
      identifier = ip + COMMON + getPort();
    }
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
