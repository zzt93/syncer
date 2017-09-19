package com.github.zzt93.syncer.config.pipeline.common;

import com.github.zzt93.syncer.common.util.FileUtil;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zzt
 */
public class Connection {

  private static final Logger logger = LoggerFactory.getLogger(Connection.class);

  private String address;
  private int port;
  private String user;
  private String passwordFile;
  private String password;

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
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
    } catch (IOException e) {
      logger.error("Invalid password file location", e);
    }
  }

  public String getPassword() {
    return password;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Connection that = (Connection) o;

    if (port != that.port) {
      return false;
    }
    return address.equals(that.address);
  }

  @Override
  public int hashCode() {
    int result = address.hashCode();
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
        ", password='" + password + '\'' +
        '}';
  }

  public boolean valid() {
    return address != null && port > 0 && port < 65536;
  }

  public String toConnectionUrl(String path) {
    return getAddress() + ":" + getPort();
  }
}
