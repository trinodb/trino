/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake;

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.Optional;

public class SnowflakeProxyConfig
{
    private String proxyHost;
    private int proxyPort = -1;
    private SnowflakeProxyProtocol proxyProtocol = SnowflakeProxyProtocol.HTTP;
    private List<String> nonProxyHosts = ImmutableList.of();
    private Optional<String> username = Optional.empty();
    private Optional<String> password = Optional.empty();

    @NotNull
    public String getProxyHost()
    {
        return proxyHost;
    }

    @ConfigDescription("Hostname of the proxy server to use")
    @Config("snowflake.proxy.host")
    public SnowflakeProxyConfig setProxyHost(String proxyHost)
    {
        this.proxyHost = proxyHost;
        return this;
    }

    public int getProxyPort()
    {
        return proxyPort;
    }

    @ConfigDescription("Port number of the proxy server to use")
    @Config("snowflake.proxy.port")
    public SnowflakeProxyConfig setProxyPort(int proxyPort)
    {
        this.proxyPort = proxyPort;
        return this;
    }

    @NotNull
    public SnowflakeProxyProtocol getProxyProtocol()
    {
        return proxyProtocol;
    }

    @ConfigDescription("Protocol used to connect to the proxy server. HTTP or HTTPS, defaults to HTTP")
    @Config("snowflake.proxy.protocol")
    public SnowflakeProxyConfig setProxyProtocol(SnowflakeProxyProtocol proxyProtocol)
    {
        this.proxyProtocol = proxyProtocol;
        return this;
    }

    @NotNull
    public List<String> getNonProxyHosts()
    {
        return nonProxyHosts;
    }

    @ConfigDescription("Lists of hosts to connect to directly, bypassing the proxy server")
    @Config("snowflake.proxy.non-proxy-hosts")
    public SnowflakeProxyConfig setNonProxyHosts(List<String> nonProxyHosts)
    {
        this.nonProxyHosts = ImmutableList.copyOf(nonProxyHosts);
        return this;
    }

    public Optional<String> getUsername()
    {
        return username;
    }

    @ConfigDescription("Username for authenticating to the proxy server")
    @Config("snowflake.proxy.username")
    public SnowflakeProxyConfig setUsername(String username)
    {
        this.username = Optional.ofNullable(username);
        return this;
    }

    public Optional<String> getPassword()
    {
        return password;
    }

    @ConfigDescription("Password for authenticating to the proxy server")
    @Config("snowflake.proxy.password")
    @ConfigSecuritySensitive
    public SnowflakeProxyConfig setPassword(String password)
    {
        this.password = Optional.ofNullable(password);
        return this;
    }

    public enum SnowflakeProxyProtocol
    {
        HTTP,
        HTTPS
    }
}
