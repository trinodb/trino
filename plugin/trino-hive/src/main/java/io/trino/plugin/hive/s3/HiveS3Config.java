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
package io.trino.plugin.hive.s3;

import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

@DefunctConfig("hive.s3.use-instance-credentials")
public class HiveS3Config
{
    private String s3AwsAccessKey;
    private String s3AwsSecretKey;
    private String s3Endpoint;
    private TrinoS3StorageClass s3StorageClass = TrinoS3StorageClass.STANDARD;
    private TrinoS3SignerType s3SignerType;
    private String s3SignerClass;
    private boolean s3PathStyleAccess;
    private String s3IamRole;
    private String s3ExternalId;
    private boolean s3SslEnabled = true;
    private boolean s3SseEnabled;
    private TrinoS3SseType s3SseType = TrinoS3SseType.S3;
    private String s3EncryptionMaterialsProvider;
    private String s3KmsKeyId;
    private String s3SseKmsKeyId;
    private int s3MaxClientRetries = 5;
    private int s3MaxErrorRetries = 10;
    private Duration s3MaxBackoffTime = new Duration(10, TimeUnit.MINUTES);
    private Duration s3MaxRetryTime = new Duration(10, TimeUnit.MINUTES);
    private Duration s3ConnectTimeout = new Duration(5, TimeUnit.SECONDS);
    private Duration s3SocketTimeout = new Duration(5, TimeUnit.SECONDS);
    private int s3MaxConnections = 500;
    private File s3StagingDirectory = new File(StandardSystemProperty.JAVA_IO_TMPDIR.value());
    private DataSize s3MultipartMinFileSize = DataSize.of(16, MEGABYTE);
    private DataSize s3MultipartMinPartSize = DataSize.of(5, MEGABYTE);
    private boolean pinS3ClientToCurrentRegion;
    private String s3UserAgentPrefix = "";
    private TrinoS3AclType s3AclType = TrinoS3AclType.PRIVATE;
    private boolean skipGlacierObjects;
    private boolean requesterPaysEnabled;
    private boolean s3StreamingUploadEnabled = true;
    private DataSize s3StreamingPartSize = DataSize.of(16, MEGABYTE);
    private String s3proxyHost;
    private Integer s3proxyPort = -1;
    private TrinoS3Protocol s3ProxyProtocol = TrinoS3Protocol.HTTPS;
    private List<String> s3nonProxyHosts = ImmutableList.of();
    private String s3proxyUsername;
    private String s3proxyPassword;
    private boolean s3preemptiveBasicProxyAuth;

    public String getS3AwsAccessKey()
    {
        return s3AwsAccessKey;
    }

    @Config("hive.s3.aws-access-key")
    public HiveS3Config setS3AwsAccessKey(String s3AwsAccessKey)
    {
        this.s3AwsAccessKey = s3AwsAccessKey;
        return this;
    }

    public String getS3AwsSecretKey()
    {
        return s3AwsSecretKey;
    }

    @Config("hive.s3.aws-secret-key")
    @ConfigSecuritySensitive
    public HiveS3Config setS3AwsSecretKey(String s3AwsSecretKey)
    {
        this.s3AwsSecretKey = s3AwsSecretKey;
        return this;
    }

    public String getS3Endpoint()
    {
        return s3Endpoint;
    }

    @Config("hive.s3.endpoint")
    public HiveS3Config setS3Endpoint(String s3Endpoint)
    {
        this.s3Endpoint = s3Endpoint;
        return this;
    }

    @NotNull
    public TrinoS3StorageClass getS3StorageClass()
    {
        return s3StorageClass;
    }

    @Config("hive.s3.storage-class")
    @ConfigDescription("AWS S3 storage class to use when writing the data")
    public HiveS3Config setS3StorageClass(TrinoS3StorageClass s3StorageClass)
    {
        this.s3StorageClass = s3StorageClass;
        return this;
    }

    public TrinoS3SignerType getS3SignerType()
    {
        return s3SignerType;
    }

    @Config("hive.s3.signer-type")
    public HiveS3Config setS3SignerType(TrinoS3SignerType s3SignerType)
    {
        this.s3SignerType = s3SignerType;
        return this;
    }

    public String getS3SignerClass()
    {
        return s3SignerClass;
    }

    @Config("hive.s3.signer-class")
    public HiveS3Config setS3SignerClass(String s3SignerClass)
    {
        this.s3SignerClass = s3SignerClass;
        return this;
    }

    public boolean isS3PathStyleAccess()
    {
        return s3PathStyleAccess;
    }

    @Config("hive.s3.path-style-access")
    @ConfigDescription("Use path-style access for all request to S3")
    public HiveS3Config setS3PathStyleAccess(boolean s3PathStyleAccess)
    {
        this.s3PathStyleAccess = s3PathStyleAccess;
        return this;
    }

    public String getS3IamRole()
    {
        return s3IamRole;
    }

    @Config("hive.s3.iam-role")
    @ConfigDescription("ARN of an IAM role to assume when connecting to S3")
    public HiveS3Config setS3IamRole(String s3IamRole)
    {
        this.s3IamRole = s3IamRole;
        return this;
    }

    public String getS3ExternalId()
    {
        return s3ExternalId;
    }

    @Config("hive.s3.external-id")
    @ConfigDescription("External ID for the IAM role trust policy when connecting to S3")
    public HiveS3Config setS3ExternalId(String s3ExternalId)
    {
        this.s3ExternalId = s3ExternalId;
        return this;
    }

    public boolean isS3SslEnabled()
    {
        return s3SslEnabled;
    }

    @Config("hive.s3.ssl.enabled")
    public HiveS3Config setS3SslEnabled(boolean s3SslEnabled)
    {
        this.s3SslEnabled = s3SslEnabled;
        return this;
    }

    public String getS3EncryptionMaterialsProvider()
    {
        return s3EncryptionMaterialsProvider;
    }

    @Config("hive.s3.encryption-materials-provider")
    @ConfigDescription("Use a custom encryption materials provider for S3 data encryption")
    public HiveS3Config setS3EncryptionMaterialsProvider(String s3EncryptionMaterialsProvider)
    {
        this.s3EncryptionMaterialsProvider = s3EncryptionMaterialsProvider;
        return this;
    }

    public String getS3KmsKeyId()
    {
        return s3KmsKeyId;
    }

    @Config("hive.s3.kms-key-id")
    @ConfigDescription("Use an AWS KMS key for S3 data encryption")
    public HiveS3Config setS3KmsKeyId(String s3KmsKeyId)
    {
        this.s3KmsKeyId = s3KmsKeyId;
        return this;
    }

    public String getS3SseKmsKeyId()
    {
        return s3SseKmsKeyId;
    }

    @Config("hive.s3.sse.kms-key-id")
    @ConfigDescription("KMS Key ID to use for S3 server-side encryption with KMS-managed key")
    public HiveS3Config setS3SseKmsKeyId(String s3SseKmsKeyId)
    {
        this.s3SseKmsKeyId = s3SseKmsKeyId;
        return this;
    }

    public boolean isS3SseEnabled()
    {
        return s3SseEnabled;
    }

    @Config("hive.s3.sse.enabled")
    @ConfigDescription("Enable S3 server side encryption")
    public HiveS3Config setS3SseEnabled(boolean s3SseEnabled)
    {
        this.s3SseEnabled = s3SseEnabled;
        return this;
    }

    @NotNull
    public TrinoS3SseType getS3SseType()
    {
        return s3SseType;
    }

    @Config("hive.s3.sse.type")
    @ConfigDescription("Key management type for S3 server-side encryption (S3 or KMS)")
    public HiveS3Config setS3SseType(TrinoS3SseType s3SseType)
    {
        this.s3SseType = s3SseType;
        return this;
    }

    @Min(0)
    public int getS3MaxClientRetries()
    {
        return s3MaxClientRetries;
    }

    @Config("hive.s3.max-client-retries")
    public HiveS3Config setS3MaxClientRetries(int s3MaxClientRetries)
    {
        this.s3MaxClientRetries = s3MaxClientRetries;
        return this;
    }

    @Min(0)
    public int getS3MaxErrorRetries()
    {
        return s3MaxErrorRetries;
    }

    @Config("hive.s3.max-error-retries")
    public HiveS3Config setS3MaxErrorRetries(int s3MaxErrorRetries)
    {
        this.s3MaxErrorRetries = s3MaxErrorRetries;
        return this;
    }

    @MinDuration("1s")
    @NotNull
    public Duration getS3MaxBackoffTime()
    {
        return s3MaxBackoffTime;
    }

    @Config("hive.s3.max-backoff-time")
    public HiveS3Config setS3MaxBackoffTime(Duration s3MaxBackoffTime)
    {
        this.s3MaxBackoffTime = s3MaxBackoffTime;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getS3MaxRetryTime()
    {
        return s3MaxRetryTime;
    }

    @Config("hive.s3.max-retry-time")
    public HiveS3Config setS3MaxRetryTime(Duration s3MaxRetryTime)
    {
        this.s3MaxRetryTime = s3MaxRetryTime;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getS3ConnectTimeout()
    {
        return s3ConnectTimeout;
    }

    @Config("hive.s3.connect-timeout")
    public HiveS3Config setS3ConnectTimeout(Duration s3ConnectTimeout)
    {
        this.s3ConnectTimeout = s3ConnectTimeout;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getS3SocketTimeout()
    {
        return s3SocketTimeout;
    }

    @Config("hive.s3.socket-timeout")
    public HiveS3Config setS3SocketTimeout(Duration s3SocketTimeout)
    {
        this.s3SocketTimeout = s3SocketTimeout;
        return this;
    }

    @Min(1)
    public int getS3MaxConnections()
    {
        return s3MaxConnections;
    }

    @Config("hive.s3.max-connections")
    public HiveS3Config setS3MaxConnections(int s3MaxConnections)
    {
        this.s3MaxConnections = s3MaxConnections;
        return this;
    }

    @NotNull
    @FileExists
    public File getS3StagingDirectory()
    {
        return s3StagingDirectory;
    }

    @Config("hive.s3.staging-directory")
    @ConfigDescription("Temporary directory for staging files before uploading to S3")
    public HiveS3Config setS3StagingDirectory(File s3StagingDirectory)
    {
        this.s3StagingDirectory = s3StagingDirectory;
        return this;
    }

    @NotNull
    @MinDataSize("16MB")
    public DataSize getS3MultipartMinFileSize()
    {
        return s3MultipartMinFileSize;
    }

    @Config("hive.s3.multipart.min-file-size")
    @ConfigDescription("Minimum file size for an S3 multipart upload")
    public HiveS3Config setS3MultipartMinFileSize(DataSize size)
    {
        this.s3MultipartMinFileSize = size;
        return this;
    }

    @NotNull
    @MinDataSize("5MB")
    public DataSize getS3MultipartMinPartSize()
    {
        return s3MultipartMinPartSize;
    }

    @Config("hive.s3.multipart.min-part-size")
    @ConfigDescription("Minimum part size for an S3 multipart upload")
    public HiveS3Config setS3MultipartMinPartSize(DataSize size)
    {
        this.s3MultipartMinPartSize = size;
        return this;
    }

    public boolean isPinS3ClientToCurrentRegion()
    {
        return pinS3ClientToCurrentRegion;
    }

    @Config("hive.s3.pin-client-to-current-region")
    @ConfigDescription("Should the S3 client be pinned to the current EC2 region")
    public HiveS3Config setPinS3ClientToCurrentRegion(boolean pinS3ClientToCurrentRegion)
    {
        this.pinS3ClientToCurrentRegion = pinS3ClientToCurrentRegion;
        return this;
    }

    @NotNull
    public String getS3UserAgentPrefix()
    {
        return s3UserAgentPrefix;
    }

    @Config("hive.s3.user-agent-prefix")
    @ConfigDescription("The user agent prefix to use for S3 calls")
    public HiveS3Config setS3UserAgentPrefix(String s3UserAgentPrefix)
    {
        this.s3UserAgentPrefix = s3UserAgentPrefix;
        return this;
    }

    @NotNull
    public TrinoS3AclType getS3AclType()
    {
        return s3AclType;
    }

    @Config("hive.s3.upload-acl-type")
    @ConfigDescription("Canned ACL type for S3 uploads")
    public HiveS3Config setS3AclType(TrinoS3AclType s3AclType)
    {
        this.s3AclType = s3AclType;
        return this;
    }

    public boolean isSkipGlacierObjects()
    {
        return skipGlacierObjects;
    }

    @Config("hive.s3.skip-glacier-objects")
    public HiveS3Config setSkipGlacierObjects(boolean skipGlacierObjects)
    {
        this.skipGlacierObjects = skipGlacierObjects;
        return this;
    }

    public boolean isRequesterPaysEnabled()
    {
        return requesterPaysEnabled;
    }

    @Config("hive.s3.requester-pays.enabled")
    public HiveS3Config setRequesterPaysEnabled(boolean requesterPaysEnabled)
    {
        this.requesterPaysEnabled = requesterPaysEnabled;
        return this;
    }

    public boolean isS3StreamingUploadEnabled()
    {
        return s3StreamingUploadEnabled;
    }

    @Config("hive.s3.streaming.enabled")
    public HiveS3Config setS3StreamingUploadEnabled(boolean s3StreamingUploadEnabled)
    {
        this.s3StreamingUploadEnabled = s3StreamingUploadEnabled;
        return this;
    }

    @NotNull
    @MinDataSize("5MB")
    @MaxDataSize("256MB")
    public DataSize getS3StreamingPartSize()
    {
        return s3StreamingPartSize;
    }

    @Config("hive.s3.streaming.part-size")
    @ConfigDescription("Part size for S3 streaming upload")
    public HiveS3Config setS3StreamingPartSize(DataSize s3StreamingPartSize)
    {
        this.s3StreamingPartSize = s3StreamingPartSize;
        return this;
    }

    public String getS3ProxyHost()
    {
        return s3proxyHost;
    }

    @Config("hive.s3.proxy.host")
    public HiveS3Config setS3ProxyHost(String s3proxyHost)
    {
        this.s3proxyHost = s3proxyHost;
        return this;
    }

    public int getS3ProxyPort()
    {
        return s3proxyPort;
    }

    @Config("hive.s3.proxy.port")
    public HiveS3Config setS3ProxyPort(int s3proxyPort)
    {
        this.s3proxyPort = s3proxyPort;
        return this;
    }

    public TrinoS3Protocol getS3ProxyProtocol()
    {
        return s3ProxyProtocol;
    }

    @Config("hive.s3.proxy.protocol")
    public HiveS3Config setS3ProxyProtocol(String s3ProxyProtocol)
    {
        this.s3ProxyProtocol = TrinoS3Protocol.valueOf(
                requireNonNull(s3ProxyProtocol, "s3ProxyProtocol is null")
                        .toUpperCase(ENGLISH));
        return this;
    }

    public List<String> getS3NonProxyHosts()
    {
        return s3nonProxyHosts;
    }

    @Config("hive.s3.proxy.non-proxy-hosts")
    public HiveS3Config setS3NonProxyHosts(List<String> s3nonProxyHosts)
    {
        this.s3nonProxyHosts = ImmutableList.copyOf(s3nonProxyHosts);
        return this;
    }

    public String getS3ProxyUsername()
    {
        return s3proxyUsername;
    }

    @Config("hive.s3.proxy.username")
    public HiveS3Config setS3ProxyUsername(String s3proxyUsername)
    {
        this.s3proxyUsername = s3proxyUsername;
        return this;
    }

    public String getS3ProxyPassword()
    {
        return s3proxyPassword;
    }

    @Config("hive.s3.proxy.password")
    public HiveS3Config setS3ProxyPassword(String s3proxyPassword)
    {
        this.s3proxyPassword = s3proxyPassword;
        return this;
    }

    public boolean getS3PreemptiveBasicProxyAuth()
    {
        return s3preemptiveBasicProxyAuth;
    }

    @Config("hive.s3.proxy.preemptive-basic-auth")
    public HiveS3Config setS3PreemptiveBasicProxyAuth(boolean s3preemptiveBasicProxyAuth)
    {
        this.s3preemptiveBasicProxyAuth = s3preemptiveBasicProxyAuth;
        return this;
    }
}
