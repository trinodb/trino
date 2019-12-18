package io.prestosql.plugin.influx;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;
import java.net.URI;

public class InfluxConfig {

    private long cacheMetaDataMillis = 10000;
    private String host = "localhost";
    private int port = 8086;
    private String database;
    private String userName;
    private String password;

    @NotNull
    public long getCacheMetaDataMillis() {
        return cacheMetaDataMillis;
    }

    @Config("cache-meta-data-millis")
    public InfluxConfig setCacheMetaDataMillis(long cacheMetaDataMillis) {
        this.cacheMetaDataMillis = cacheMetaDataMillis;
        return this;
    }

    @NotNull
    public String getHost()
    {
        return host;
    }

    @Config("host")
    public InfluxConfig setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    @Config("port")
    public InfluxConfig setPort(int port) {
        this.port = port;
        return this;
    }

    @NotNull
    public String getDatabase() {
        return database;
    }

    @Config("database")
    public InfluxConfig setDatabase(String database) {
        this.database = database;
        return this;
    }

    @NotNull
    public String getUserName() {
        return userName;
    }

    @Config("user")
    public InfluxConfig setUserName(String userName) {
        this.userName = userName;
        return this;
    }

    @NotNull
    public String getPassword() {
        return password;
    }

    @Config("password")
    public InfluxConfig setPassword(String password) {
        this.password = password;
        return this;
    }
}
