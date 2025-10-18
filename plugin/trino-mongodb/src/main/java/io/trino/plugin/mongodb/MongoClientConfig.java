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
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigHidden;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import static java.util.concurrent.TimeUnit.SECONDS;

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

    // query configurations
    private int cursorBatchSize; // use driver default

    private ReadPreferenceType readPreference = ReadPreferenceType.PRIMARY;
    private WriteConcernType writeConcern = WriteConcernType.ACKNOWLEDGED;
    private String requiredReplicaSetName;
    private String implicitRowFieldPrefix = "_pos";
    private boolean projectionPushDownEnabled = true;
    private boolean allowLocalScheduling;
    private Duration dynamicFilteringWaitTimeout = new Duration(5, SECONDS);

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

    @ConfigHidden
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

    public boolean isProjectionPushdownEnabled()
    {
        return projectionPushDownEnabled;
    }

    @Config("mongodb.projection-pushdown-enabled")
    @ConfigDescription("Read only required fields from a row type")
    public MongoClientConfig setProjectionPushdownEnabled(boolean projectionPushDownEnabled)
    {
        this.projectionPushDownEnabled = projectionPushDownEnabled;
        return this;
    }

    public boolean isAllowLocalScheduling()
    {
        return allowLocalScheduling;
    }

    @Config("mongodb.allow-local-scheduling")
    @ConfigDescription("Assign MongoDB splits to a specific host if worker and MongoDB share the same cluster")
    public MongoClientConfig setAllowLocalScheduling(boolean allowLocalScheduling)
    {
        this.allowLocalScheduling = allowLocalScheduling;
        return this;
    }

    @MinDuration("0ms")
    @NotNull
    public Duration getDynamicFilteringWaitTimeout()
    {
        return dynamicFilteringWaitTimeout;
    }

    @Config("mongodb.dynamic-filtering.wait-timeout")
    @ConfigDescription("Duration to wait for completion of dynamic filters during split generation")
    public MongoClientConfig setDynamicFilteringWaitTimeout(Duration dynamicFilteringWaitTimeout)
    {
        this.dynamicFilteringWaitTimeout = dynamicFilteringWaitTimeout;
        return this;
    }
}
