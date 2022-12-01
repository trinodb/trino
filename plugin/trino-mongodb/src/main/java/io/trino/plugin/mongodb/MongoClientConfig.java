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
package io.trino.plugin.mongodb;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.configuration.validation.FileExists;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import java.io.File;
import java.util.Optional;

@DefunctConfig({"mongodb.connection-per-host", "mongodb.socket-keep-alive", "mongodb.seeds", "mongodb.credentials"})
public class MongoClientConfig
{
    private String schemaCollection = "_schema";
    private boolean caseInsensitiveNameMatching;
    private String connectionUrl;

    private int minConnectionsPerHost;
    private int connectionsPerHost = 100;
    private int maxWaitTime = 120_000;
    private int connectionTimeout = 10_000;
    private int socketTimeout;
    private int maxConnectionIdleTime;
    private boolean tlsEnabled;
    private File keystorePath;
    private String keystorePassword;
    private File truststorePath;
    private String truststorePassword;

    // query configurations
    private int cursorBatchSize; // use driver default

    private ReadPreferenceType readPreference = ReadPreferenceType.PRIMARY;
    private WriteConcernType writeConcern = WriteConcernType.ACKNOWLEDGED;
    private String requiredReplicaSetName;
    private String implicitRowFieldPrefix = "_pos";

    @AssertTrue(message = "'mongodb.tls.keystore-path', 'mongodb.tls.keystore-password', 'mongodb.tls.truststore-path' and 'mongodb.tls.truststore-password' must be empty when TLS is disabled")
    public boolean isValidTlsConfig()
    {
        if (!tlsEnabled) {
            return keystorePath == null && keystorePassword == null && truststorePath == null && truststorePassword == null;
        }
        return true;
    }

    @NotNull
    public String getSchemaCollection()
    {
        return schemaCollection;
    }

    @Config("mongodb.schema-collection")
    public MongoClientConfig setSchemaCollection(String schemaCollection)
    {
        this.schemaCollection = schemaCollection;
        return this;
    }

    public boolean isCaseInsensitiveNameMatching()
    {
        return caseInsensitiveNameMatching;
    }

    @Config("mongodb.case-insensitive-name-matching")
    public MongoClientConfig setCaseInsensitiveNameMatching(boolean caseInsensitiveNameMatching)
    {
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        return this;
    }

    @NotNull
    public @Pattern(message = "Invalid connection URL. Expected mongodb:// or mongodb+srv://", regexp = "^mongodb(\\+srv)?://.*") String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("mongodb.connection-url")
    @ConfigSecuritySensitive
    public MongoClientConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    @Min(0)
    public int getMinConnectionsPerHost()
    {
        return minConnectionsPerHost;
    }

    @Config("mongodb.min-connections-per-host")
    public MongoClientConfig setMinConnectionsPerHost(int minConnectionsPerHost)
    {
        this.minConnectionsPerHost = minConnectionsPerHost;
        return this;
    }

    @Min(1)
    public int getConnectionsPerHost()
    {
        return connectionsPerHost;
    }

    @Config("mongodb.connections-per-host")
    public MongoClientConfig setConnectionsPerHost(int connectionsPerHost)
    {
        this.connectionsPerHost = connectionsPerHost;
        return this;
    }

    @Min(0)
    public int getMaxWaitTime()
    {
        return maxWaitTime;
    }

    @Config("mongodb.max-wait-time")
    public MongoClientConfig setMaxWaitTime(int maxWaitTime)
    {
        this.maxWaitTime = maxWaitTime;
        return this;
    }

    @Min(0)
    public int getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("mongodb.connection-timeout")
    public MongoClientConfig setConnectionTimeout(int connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    @Min(0)
    public int getSocketTimeout()
    {
        return socketTimeout;
    }

    @Config("mongodb.socket-timeout")
    public MongoClientConfig setSocketTimeout(int socketTimeout)
    {
        this.socketTimeout = socketTimeout;
        return this;
    }

    @NotNull
    public ReadPreferenceType getReadPreference()
    {
        return readPreference;
    }

    @Config("mongodb.read-preference")
    public MongoClientConfig setReadPreference(ReadPreferenceType readPreference)
    {
        this.readPreference = readPreference;
        return this;
    }

    @NotNull
    public WriteConcernType getWriteConcern()
    {
        return writeConcern;
    }

    @Config("mongodb.write-concern")
    public MongoClientConfig setWriteConcern(WriteConcernType writeConcern)
    {
        this.writeConcern = writeConcern;
        return this;
    }

    public String getRequiredReplicaSetName()
    {
        return requiredReplicaSetName;
    }

    @Config("mongodb.required-replica-set")
    public MongoClientConfig setRequiredReplicaSetName(String requiredReplicaSetName)
    {
        this.requiredReplicaSetName = requiredReplicaSetName;
        return this;
    }

    public int getCursorBatchSize()
    {
        return cursorBatchSize;
    }

    @Config("mongodb.cursor-batch-size")
    public MongoClientConfig setCursorBatchSize(int cursorBatchSize)
    {
        this.cursorBatchSize = cursorBatchSize;
        return this;
    }

    @NotNull
    public String getImplicitRowFieldPrefix()
    {
        return implicitRowFieldPrefix;
    }

    @Config("mongodb.implicit-row-field-prefix")
    public MongoClientConfig setImplicitRowFieldPrefix(String implicitRowFieldPrefix)
    {
        this.implicitRowFieldPrefix = implicitRowFieldPrefix;
        return this;
    }

    public boolean getTlsEnabled()
    {
        return this.tlsEnabled;
    }

    @Config("mongodb.tls.enabled")
    @LegacyConfig("mongodb.ssl.enabled")
    public MongoClientConfig setTlsEnabled(boolean tlsEnabled)
    {
        this.tlsEnabled = tlsEnabled;
        return this;
    }

    public Optional<@FileExists File> getKeystorePath()
    {
        return Optional.ofNullable(keystorePath);
    }

    @Config("mongodb.tls.keystore-path")
    public MongoClientConfig setKeystorePath(File keystorePath)
    {
        this.keystorePath = keystorePath;
        return this;
    }

    public Optional<String> getKeystorePassword()
    {
        return Optional.ofNullable(keystorePassword);
    }

    @Config("mongodb.tls.keystore-password")
    @ConfigSecuritySensitive
    public MongoClientConfig setKeystorePassword(String keystorePassword)
    {
        this.keystorePassword = keystorePassword;
        return this;
    }

    public Optional<@FileExists File> getTruststorePath()
    {
        return Optional.ofNullable(truststorePath);
    }

    @Config("mongodb.tls.truststore-path")
    public MongoClientConfig setTruststorePath(File truststorePath)
    {
        this.truststorePath = truststorePath;
        return this;
    }

    public Optional<String> getTruststorePassword()
    {
        return Optional.ofNullable(truststorePassword);
    }

    @Config("mongodb.tls.truststore-password")
    @ConfigSecuritySensitive
    public MongoClientConfig setTruststorePassword(String truststorePassword)
    {
        this.truststorePassword = truststorePassword;
        return this;
    }

    @Min(0)
    public int getMaxConnectionIdleTime()
    {
        return maxConnectionIdleTime;
    }

    @Config("mongodb.max-connection-idle-time")
    public MongoClientConfig setMaxConnectionIdleTime(int maxConnectionIdleTime)
    {
        this.maxConnectionIdleTime = maxConnectionIdleTime;
        return this;
    }
}
