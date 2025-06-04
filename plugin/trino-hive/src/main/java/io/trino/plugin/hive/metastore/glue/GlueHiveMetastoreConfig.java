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
package io.trino.plugin.hive.metastore.glue;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.DefunctConfig;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import java.util.Optional;

@DefunctConfig({
        "hive.metastore.glue.use-instance-credentials",
        "hive.metastore.glue.get-partition-threads",
        "hive.metastore.glue.read-statistics-threads",
        "hive.metastore.glue.write-statistics-threads",
        "hive.metastore.glue.proxy-api-id",
        "hive.metastore.glue.aws-credentials-provider",
})
public class GlueHiveMetastoreConfig
{
    private Optional<String> glueRegion = Optional.empty();
    private Optional<String> glueEndpointUrl = Optional.empty();
    private Optional<String> glueStsRegion = Optional.empty();
    private Optional<String> glueStsEndpointUrl = Optional.empty();
    private boolean pinGlueClientToCurrentRegion;
    private int maxGlueErrorRetries = 10;
    private int maxGlueConnections = 30;
    private Optional<String> defaultWarehouseDir = Optional.empty();
    private Optional<String> iamRole = Optional.empty();
    private Optional<String> externalId = Optional.empty();
    private Optional<String> awsAccessKey = Optional.empty();
    private Optional<String> awsSecretKey = Optional.empty();
    private boolean useWebIdentityTokenCredentialsProvider;
    private Optional<String> catalogId = Optional.empty();
    private int partitionSegments = 5;
    private int threads = 40;
    private boolean assumeCanonicalPartitionKeys;
    private boolean skipArchive;

    public Optional<String> getGlueRegion()
    {
        return glueRegion;
    }

    @Config("hive.metastore.glue.region")
    @ConfigDescription("AWS Region for Glue Data Catalog")
    public GlueHiveMetastoreConfig setGlueRegion(String region)
    {
        this.glueRegion = Optional.ofNullable(region);
        return this;
    }

    public Optional<String> getGlueEndpointUrl()
    {
        return glueEndpointUrl;
    }

    @Config("hive.metastore.glue.endpoint-url")
    @ConfigDescription("Glue API endpoint URL")
    public GlueHiveMetastoreConfig setGlueEndpointUrl(String glueEndpointUrl)
    {
        this.glueEndpointUrl = Optional.ofNullable(glueEndpointUrl);
        return this;
    }

    public Optional<String> getGlueStsRegion()
    {
        return glueStsRegion;
    }

    @Config("hive.metastore.glue.sts.region")
    @ConfigDescription("AWS STS signing region for Glue authentication")
    public GlueHiveMetastoreConfig setGlueStsRegion(String glueStsRegion)
    {
        this.glueStsRegion = Optional.ofNullable(glueStsRegion);
        return this;
    }

    public Optional<String> getGlueStsEndpointUrl()
    {
        return glueStsEndpointUrl;
    }

    @Config("hive.metastore.glue.sts.endpoint")
    @ConfigDescription("AWS STS endpoint for Glue authentication")
    public GlueHiveMetastoreConfig setGlueStsEndpointUrl(String glueStsEndpointUrl)
    {
        this.glueStsEndpointUrl = Optional.ofNullable(glueStsEndpointUrl);
        return this;
    }

    public boolean getPinGlueClientToCurrentRegion()
    {
        return pinGlueClientToCurrentRegion;
    }

    @Config("hive.metastore.glue.pin-client-to-current-region")
    @ConfigDescription("Should the Glue client be pinned to the current EC2 region")
    public GlueHiveMetastoreConfig setPinGlueClientToCurrentRegion(boolean pinGlueClientToCurrentRegion)
    {
        this.pinGlueClientToCurrentRegion = pinGlueClientToCurrentRegion;
        return this;
    }

    @Min(1)
    public int getMaxGlueConnections()
    {
        return maxGlueConnections;
    }

    @Config("hive.metastore.glue.max-connections")
    @ConfigDescription("Max number of concurrent connections to Glue")
    public GlueHiveMetastoreConfig setMaxGlueConnections(int maxGlueConnections)
    {
        this.maxGlueConnections = maxGlueConnections;
        return this;
    }

    @Min(0)
    public int getMaxGlueErrorRetries()
    {
        return maxGlueErrorRetries;
    }

    @Config("hive.metastore.glue.max-error-retries")
    @ConfigDescription("Maximum number of error retries for the Glue client")
    public GlueHiveMetastoreConfig setMaxGlueErrorRetries(int maxGlueErrorRetries)
    {
        this.maxGlueErrorRetries = maxGlueErrorRetries;
        return this;
    }

    public Optional<String> getDefaultWarehouseDir()
    {
        return defaultWarehouseDir;
    }

    @Config("hive.metastore.glue.default-warehouse-dir")
    @ConfigDescription("Hive Glue metastore default warehouse directory")
    public GlueHiveMetastoreConfig setDefaultWarehouseDir(String defaultWarehouseDir)
    {
        this.defaultWarehouseDir = Optional.ofNullable(defaultWarehouseDir);
        return this;
    }

    public Optional<String> getIamRole()
    {
        return iamRole;
    }

    @Config("hive.metastore.glue.iam-role")
    @ConfigDescription("ARN of an IAM role to assume when connecting to Glue")
    public GlueHiveMetastoreConfig setIamRole(String iamRole)
    {
        this.iamRole = Optional.ofNullable(iamRole);
        return this;
    }

    public Optional<String> getExternalId()
    {
        return externalId;
    }

    @Config("hive.metastore.glue.external-id")
    @ConfigDescription("External ID for the IAM role trust policy when connecting to Glue")
    public GlueHiveMetastoreConfig setExternalId(String externalId)
    {
        this.externalId = Optional.ofNullable(externalId);
        return this;
    }

    public Optional<String> getAwsAccessKey()
    {
        return awsAccessKey;
    }

    @Config("hive.metastore.glue.aws-access-key")
    @ConfigDescription("Hive Glue metastore AWS access key")
    public GlueHiveMetastoreConfig setAwsAccessKey(String awsAccessKey)
    {
        this.awsAccessKey = Optional.ofNullable(awsAccessKey);
        return this;
    }

    public Optional<String> getAwsSecretKey()
    {
        return awsSecretKey;
    }

    @Config("hive.metastore.glue.aws-secret-key")
    @ConfigDescription("Hive Glue metastore AWS secret key")
    @ConfigSecuritySensitive
    public GlueHiveMetastoreConfig setAwsSecretKey(String awsSecretKey)
    {
        this.awsSecretKey = Optional.ofNullable(awsSecretKey);
        return this;
    }

    public boolean isUseWebIdentityTokenCredentialsProvider()
    {
        return useWebIdentityTokenCredentialsProvider;
    }

    @Config("hive.metastore.glue.use-web-identity-token-credentials-provider")
    @ConfigDescription("If true, explicitly use the WebIdentityTokenCredentialsProvider" +
            " instead of the default credential provider chain.")
    public GlueHiveMetastoreConfig setUseWebIdentityTokenCredentialsProvider(boolean useWebIdentityTokenCredentialsProvider)
    {
        this.useWebIdentityTokenCredentialsProvider = useWebIdentityTokenCredentialsProvider;
        return this;
    }

    public Optional<String> getCatalogId()
    {
        return catalogId;
    }

    @Config("hive.metastore.glue.catalogid")
    @ConfigDescription("Hive Glue metastore catalog id")
    public GlueHiveMetastoreConfig setCatalogId(String catalogId)
    {
        this.catalogId = Optional.ofNullable(catalogId);
        return this;
    }

    @Min(1)
    @Max(10)
    public int getPartitionSegments()
    {
        return partitionSegments;
    }

    @Config("hive.metastore.glue.partitions-segments")
    @ConfigDescription("Number of segments for partitioned Glue tables")
    public GlueHiveMetastoreConfig setPartitionSegments(int partitionSegments)
    {
        this.partitionSegments = partitionSegments;
        return this;
    }

    @Min(1)
    public int getThreads()
    {
        return threads;
    }

    @Config("hive.metastore.glue.threads")
    @ConfigDescription("Number of threads for parallel operations")
    public GlueHiveMetastoreConfig setThreads(int threads)
    {
        this.threads = threads;
        return this;
    }

    public boolean isAssumeCanonicalPartitionKeys()
    {
        return assumeCanonicalPartitionKeys;
    }

    @Config("hive.metastore.glue.assume-canonical-partition-keys")
    @ConfigDescription("Allow conversion of non-char types (eg BIGINT, timestamp) to canonical string formats")
    public GlueHiveMetastoreConfig setAssumeCanonicalPartitionKeys(boolean assumeCanonicalPartitionKeys)
    {
        this.assumeCanonicalPartitionKeys = assumeCanonicalPartitionKeys;
        return this;
    }

    public boolean isSkipArchive()
    {
        return skipArchive;
    }

    @Config("hive.metastore.glue.skip-archive")
    @ConfigDescription("Skip archiving an old table version when updating a table in the Glue metastore")
    public GlueHiveMetastoreConfig setSkipArchive(boolean skipArchive)
    {
        this.skipArchive = skipArchive;
        return this;
    }
}
