/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.bigquery;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.ConfigurationException;
import com.google.inject.spi.Message;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import jakarta.annotation.PostConstruct;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.net.URI;
import java.util.Optional;

import static com.google.common.base.Strings.isNullOrEmpty;

public class BigQueryProxyConfig
{
    private URI uri;
    private Optional<String> username = Optional.empty();
    private Optional<String> password = Optional.empty();
    private File keystorePath;
    private String keystorePassword;
    private File truststorePath;
    private String truststorePassword;

    @NotNull
    public URI getUri()
    {
        return uri;
    }

    @ConfigDescription("Proxy URI (host and port only)")
    @Config("bigquery.rpc-proxy.uri")
    public BigQueryProxyConfig setUri(URI uri)
    {
        this.uri = uri;
        return this;
    }

    public Optional<String> getUsername()
    {
        return username;
    }

    @ConfigDescription("Username used to authenticate against proxy")
    @Config("bigquery.rpc-proxy.username")
    public BigQueryProxyConfig setUsername(String username)
    {
        this.username = Optional.ofNullable(username);
        return this;
    }

    public Optional<String> getPassword()
    {
        return password;
    }

    @ConfigSecuritySensitive
    @ConfigDescription("Password used to authenticate against proxy")
    @Config("bigquery.rpc-proxy.password")
    public BigQueryProxyConfig setPassword(String password)
    {
        this.password = Optional.ofNullable(password);
        return this;
    }

    public Optional<@FileExists File> getKeystorePath()
    {
        return Optional.ofNullable(keystorePath);
    }

    @Config("bigquery.rpc-proxy.keystore-path")
    @ConfigDescription("Path to a Java keystore file")
    public BigQueryProxyConfig setKeystorePath(File keystorePath)
    {
        this.keystorePath = keystorePath;
        return this;
    }

    public Optional<String> getKeystorePassword()
    {
        return Optional.ofNullable(keystorePassword);
    }

    @Config("bigquery.rpc-proxy.keystore-password")
    @ConfigDescription("Password to a Java keystore file")
    @ConfigSecuritySensitive
    public BigQueryProxyConfig setKeystorePassword(String keystorePassword)
    {
        this.keystorePassword = keystorePassword;
        return this;
    }

    public Optional<@FileExists File> getTruststorePath()
    {
        return Optional.ofNullable(truststorePath);
    }

    @Config("bigquery.rpc-proxy.truststore-path")
    @ConfigDescription("Path to a Java truststore file")
    public BigQueryProxyConfig setTruststorePath(File truststorePath)
    {
        this.truststorePath = truststorePath;
        return this;
    }

    public Optional<String> getTruststorePassword()
    {
        return Optional.ofNullable(truststorePassword);
    }

    @Config("bigquery.rpc-proxy.truststore-password")
    @ConfigDescription("Password to a Java truststore file")
    @ConfigSecuritySensitive
    public BigQueryProxyConfig setTruststorePassword(String truststorePassword)
    {
        this.truststorePassword = truststorePassword;
        return this;
    }

    @PostConstruct
    @VisibleForTesting
    void validate()
    {
        if (!isNullOrEmpty(uri.getPath())) {
            throw exception("BigQuery RPC proxy URI cannot specify path");
        }

        if ((username.isPresent() && password.isEmpty())) {
            throw exception("bigquery.rpc-proxy.username was set but bigquery.rpc-proxy.password is empty");
        }

        if (username.isEmpty() && password.isPresent()) {
            throw exception("bigquery.rpc-proxy.password was set but bigquery.rpc-proxy.username is empty");
        }
    }

    private static ConfigurationException exception(String message)
    {
        return new ConfigurationException(ImmutableList.of(new Message(message)));
    }
}
