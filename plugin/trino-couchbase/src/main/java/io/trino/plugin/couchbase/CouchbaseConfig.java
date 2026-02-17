package io.trino.plugin.couchbase;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

public class CouchbaseConfig
{
    private String cluster = "localhost";
    private String username = "Administrator";
    private String password = "password";
    private String tlsKey;
    private String tlsKeyPassword;
    private String tlsCertificate;
    private String schemaFolder = "couchbase-schema";
    private String bucket = "default";
    private String scope = "_default";

    @Config("couchbase.cluster")
    @ConfigDescription("Couchbase cluster connection string")
    public CouchbaseConfig setCluster(String connstring)
    {
        this.cluster = connstring;
        return this;
    }

    public String getCluster()
    {
        return cluster;
    }

    @Config("couchbase.username")
    @ConfigDescription("Username for the cluster")
    public CouchbaseConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public String getUsername()
    {
        return username;
    }

    @Config("couchbase.password")
    @ConfigDescription("Password for the cluster")
    @ConfigSecuritySensitive
    public CouchbaseConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    @Config("couchbase.tls-key")
    @ConfigDescription("Key file address for mTls")
    public CouchbaseConfig setTlsKey(String tlsKey)
    {
        this.tlsKey = tlsKey;
        return this;
    }

    public String getTlsKey()
    {
        return tlsKey;
    }

    @Config("couchbase.tls-key-password")
    @ConfigDescription("Key password")
    @ConfigSecuritySensitive
    public CouchbaseConfig setTlsKeyPassword(String password)
    {
        this.tlsKeyPassword = password;
        return this;
    }

    public String getTlsKeyPassword()
    {
    return tlsKeyPassword;
    }

    @Config("couchbase.tls-certificate")
    @ConfigDescription("Cluster root certificate file address")
    public CouchbaseConfig setTlsCertificate(String certificate)
    {
        this.tlsCertificate = certificate;
        return this;
    }

    public String getTlsCertificate()
    {
        return tlsCertificate;
    }

    @Config("couchbase.schema-folder")
    @ConfigDescription("Path for folder with json files containing Trino schema mappings")
    public CouchbaseConfig setSchemaFolder(String schemaFolder) {
        this.schemaFolder = schemaFolder;
        return this;
    }

    public String getSchemaFolder()
    {
        return schemaFolder;
    }

    @Config("couchbase.bucket")
    @ConfigDescription("Bucket to connect to")
    public CouchbaseConfig setBucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    public String getBucket() {
        return bucket;
    }

    @Config("couchbase.scope")
    @ConfigDescription("Scope to connect to")
    public CouchbaseConfig setScope(String scope) {
        this.scope = scope;
        return this;
    }

    public String getScope() {
        return scope;
    }
}
