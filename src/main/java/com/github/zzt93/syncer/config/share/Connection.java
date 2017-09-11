package com.github.zzt93.syncer.config.share;

import com.github.zzt93.syncer.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author zzt
 */
public class Connection {
    private static final Logger logger = LoggerFactory.getLogger(Connection.class);

    private String address;
    private int port;
    private String user;
    private String passwordFile;

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

    private String password;

    public void setPasswordFile(String passwordFile) {
        this.passwordFile = passwordFile;
        try {
            this.password = FileUtil.readAll(passwordFile);
        } catch (IOException e) {
            logger.error("Invalid password file location", e);
        }
    }

    public String getPasswordFile() {
        return passwordFile;
    }

    String getPassword() {
        return password;
    }


}
