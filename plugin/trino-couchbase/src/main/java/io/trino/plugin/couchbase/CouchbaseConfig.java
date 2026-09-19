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
package io.trino.plugin.couchbase;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.SECONDS;

public class CouchbaseConfig
{
    private String cluster = "couchbase://localhost";
    private String username;
    private String password;
    private String tlsKey;
    private String tlsKeyPassword;
    private String tlsCertificate;
    private String schemaFolder = "couchbase-schema";
    private String bucket = "default";
    private String scope = "_default";
    private Duration timeouts = new Duration(60, SECONDS);
    private long pageSize = 5000;

    @NotNull
    public String getCluster()
    {
        return cluster;
    }

    @Config("couchbase.cluster")
    @ConfigDescription("Couchbase cluster connection string")
    public CouchbaseConfig setCluster(String cluster)
    {
        this.cluster = cluster;
        return this;
    }

    @NotNull
    public String getUsername()
    {
        return username;
    }

    @Config("couchbase.username")
    @ConfigDescription("Username for the cluster")
    public CouchbaseConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    @NotNull
    public String getPassword()
    {
        return password;
    }

    @Config("couchbase.password")
    @ConfigDescription("Password for the cluster")
    @ConfigSecuritySensitive
    public CouchbaseConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public String getTlsKey()
    {
        return tlsKey;
    }

    @Config("couchbase.tls-key")
    @ConfigDescription("Key file address for mTls")
    public CouchbaseConfig setTlsKey(String tlsKey)
    {
        this.tlsKey = tlsKey;
        return this;
    }

    public String getTlsKeyPassword()
    {
        return tlsKeyPassword;
    }

    @Config("couchbase.tls-key-password")
    @ConfigDescription("Key password")
    @ConfigSecuritySensitive
    public CouchbaseConfig setTlsKeyPassword(String tlsKeyPassword)
    {
        this.tlsKeyPassword = tlsKeyPassword;
        return this;
    }

    public String getTlsCertificate()
    {
        return tlsCertificate;
    }

    @Config("couchbase.tls-certificate")
    @ConfigDescription("Cluster root certificate file address")
    public CouchbaseConfig setTlsCertificate(String tlsCertificate)
    {
        this.tlsCertificate = tlsCertificate;
        return this;
    }

    @NotNull
    public String getSchemaFolder()
    {
        return schemaFolder;
    }

    @Config("couchbase.schema-folder")
    @ConfigDescription("Path for folder with json files containing Trino schema mappings")
    public CouchbaseConfig setSchemaFolder(String schemaFolder)
    {
        this.schemaFolder = schemaFolder;
        return this;
    }

    @NotNull
    public String getBucket()
    {
        return bucket;
    }

    @Config("couchbase.bucket")
    @ConfigDescription("Bucket to connect to")
    public CouchbaseConfig setBucket(String bucket)
    {
        this.bucket = bucket;
        return this;
    }

    @NotNull
    public String getScope()
    {
        return scope;
    }

    @Config("couchbase.scope")
    @ConfigDescription("Scope to connect to")
    public CouchbaseConfig setScope(String scope)
    {
        this.scope = scope;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getTimeouts()
    {
        return timeouts;
    }

    @Config("couchbase.timeouts")
    @ConfigDescription("Operations timeout; defaults to 1m")
    public CouchbaseConfig setTimeouts(Duration timeouts)
    {
        this.timeouts = timeouts;
        return this;
    }

    public long getPageSize()
    {
        return pageSize;
    }

    @Config("couchbase.page-size")
    @ConfigDescription("Maximum number of rows to be fetched in a single query")
    public CouchbaseConfig setPageSize(long pageSize)
    {
        this.pageSize = pageSize;
        return this;
    }
}
