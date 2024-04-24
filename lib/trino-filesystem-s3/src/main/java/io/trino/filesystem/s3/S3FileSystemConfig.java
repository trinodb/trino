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
package io.trino.filesystem.s3;

import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class S3FileSystemConfig
{
    public enum S3SseType
    {
        NONE, S3, KMS
    }

    public enum ObjectCannedAcl
    {
        NONE,
        PRIVATE,
        PUBLIC_READ,
        PUBLIC_READ_WRITE,
        AUTHENTICATED_READ,
        BUCKET_OWNER_READ,
        BUCKET_OWNER_FULL_CONTROL;

        public static ObjectCannedACL getCannedAcl(S3FileSystemConfig.ObjectCannedAcl cannedAcl)
        {
            return switch (cannedAcl) {
                case NONE -> null;
                case PRIVATE -> ObjectCannedACL.PRIVATE;
                case PUBLIC_READ -> ObjectCannedACL.PUBLIC_READ;
                case PUBLIC_READ_WRITE -> ObjectCannedACL.PUBLIC_READ_WRITE;
                case AUTHENTICATED_READ -> ObjectCannedACL.AUTHENTICATED_READ;
                case BUCKET_OWNER_READ -> ObjectCannedACL.BUCKET_OWNER_READ;
                case BUCKET_OWNER_FULL_CONTROL -> ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL;
            };
        }
    }

    private String awsAccessKey;
    private String awsSecretKey;
    private String endpoint;
    private String region;
    private boolean pathStyleAccess;
    private String iamRole;
    private String roleSessionName = "trino-filesystem";
    private String externalId;
    private String stsEndpoint;
    private String stsRegion;
    private S3SseType sseType = S3SseType.NONE;
    private String sseKmsKeyId;
    private DataSize streamingPartSize = DataSize.of(16, MEGABYTE);
    private boolean requesterPays;
    private Integer maxConnections;
    private Duration connectionTtl;
    private Duration connectionMaxIdleTime;
    private Duration socketConnectTimeout;
    private Duration socketReadTimeout;
    private boolean tcpKeepAlive;
    private HostAndPort httpProxy;
    private boolean httpProxySecure;
    private ObjectCannedAcl objectCannedAcl = ObjectCannedAcl.NONE;

    public String getAwsAccessKey()
    {
        return awsAccessKey;
    }

    @Config("s3.aws-access-key")
    public S3FileSystemConfig setAwsAccessKey(String awsAccessKey)
    {
        this.awsAccessKey = awsAccessKey;
        return this;
    }

    public String getAwsSecretKey()
    {
        return awsSecretKey;
    }

    @Config("s3.aws-secret-key")
    @ConfigSecuritySensitive
    public S3FileSystemConfig setAwsSecretKey(String awsSecretKey)
    {
        this.awsSecretKey = awsSecretKey;
        return this;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    @Config("s3.endpoint")
    public S3FileSystemConfig setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    public String getRegion()
    {
        return region;
    }

    @Config("s3.region")
    public S3FileSystemConfig setRegion(String region)
    {
        this.region = region;
        return this;
    }

    public boolean isPathStyleAccess()
    {
        return pathStyleAccess;
    }

    @Config("s3.path-style-access")
    @ConfigDescription("Use path-style access for all requests to S3")
    public S3FileSystemConfig setPathStyleAccess(boolean pathStyleAccess)
    {
        this.pathStyleAccess = pathStyleAccess;
        return this;
    }

    public String getIamRole()
    {
        return iamRole;
    }

    @Config("s3.iam-role")
    @ConfigDescription("ARN of an IAM role to assume when connecting to S3")
    public S3FileSystemConfig setIamRole(String iamRole)
    {
        this.iamRole = iamRole;
        return this;
    }

    @NotNull
    public String getRoleSessionName()
    {
        return roleSessionName;
    }

    @Config("s3.role-session-name")
    @ConfigDescription("Role session name to use when connecting to S3")
    public S3FileSystemConfig setRoleSessionName(String roleSessionName)
    {
        this.roleSessionName = roleSessionName;
        return this;
    }

    public String getExternalId()
    {
        return externalId;
    }

    @Config("s3.external-id")
    @ConfigDescription("External ID for the IAM role trust policy when connecting to S3")
    public S3FileSystemConfig setExternalId(String externalId)
    {
        this.externalId = externalId;
        return this;
    }

    public String getStsEndpoint()
    {
        return stsEndpoint;
    }

    @Config("s3.sts.endpoint")
    public S3FileSystemConfig setStsEndpoint(String stsEndpoint)
    {
        this.stsEndpoint = stsEndpoint;
        return this;
    }

    public String getStsRegion()
    {
        return stsRegion;
    }

    @Config("s3.sts.region")
    public S3FileSystemConfig setStsRegion(String stsRegion)
    {
        this.stsRegion = stsRegion;
        return this;
    }

    @NotNull
    public ObjectCannedAcl getCannedAcl()
    {
        return objectCannedAcl;
    }

    @Config("s3.canned-acl")
    @ConfigDescription("Canned ACL (predefined grants) to manage access to objects")
    public S3FileSystemConfig setCannedAcl(ObjectCannedAcl objectCannedAcl)
    {
        this.objectCannedAcl = objectCannedAcl;
        return this;
    }

    @NotNull
    public S3SseType getSseType()
    {
        return sseType;
    }

    @Config("s3.sse.type")
    public S3FileSystemConfig setSseType(S3SseType sseType)
    {
        this.sseType = sseType;
        return this;
    }

    public String getSseKmsKeyId()
    {
        return sseKmsKeyId;
    }

    @Config("s3.sse.kms-key-id")
    @ConfigDescription("KMS Key ID to use for S3 server-side encryption with KMS-managed key")
    public S3FileSystemConfig setSseKmsKeyId(String sseKmsKeyId)
    {
        this.sseKmsKeyId = sseKmsKeyId;
        return this;
    }

    @NotNull
    @MinDataSize("5MB")
    @MaxDataSize("256MB")
    public DataSize getStreamingPartSize()
    {
        return streamingPartSize;
    }

    @Config("s3.streaming.part-size")
    @ConfigDescription("Part size for S3 streaming upload")
    public S3FileSystemConfig setStreamingPartSize(DataSize streamingPartSize)
    {
        this.streamingPartSize = streamingPartSize;
        return this;
    }

    public boolean isRequesterPays()
    {
        return requesterPays;
    }

    @Config("s3.requester-pays")
    public S3FileSystemConfig setRequesterPays(boolean requesterPays)
    {
        this.requesterPays = requesterPays;
        return this;
    }

    @Min(1)
    public Integer getMaxConnections()
    {
        return maxConnections;
    }

    @Config("s3.max-connections")
    public S3FileSystemConfig setMaxConnections(Integer maxConnections)
    {
        this.maxConnections = maxConnections;
        return this;
    }

    public Optional<Duration> getConnectionTtl()
    {
        return Optional.ofNullable(connectionTtl);
    }

    @Config("s3.connection-ttl")
    @ConfigDescription("Maximum time allowed for connections to be reused before being replaced in the connection pool")
    public S3FileSystemConfig setConnectionTtl(Duration connectionTtl)
    {
        this.connectionTtl = connectionTtl;
        return this;
    }

    public Optional<Duration> getConnectionMaxIdleTime()
    {
        return Optional.ofNullable(connectionMaxIdleTime);
    }

    @Config("s3.connection-max-idle-time")
    @ConfigDescription("Maximum time allowed for connections to remain idle in the connection pool before being closed")
    public S3FileSystemConfig setConnectionMaxIdleTime(Duration connectionMaxIdleTime)
    {
        this.connectionMaxIdleTime = connectionMaxIdleTime;
        return this;
    }

    public Optional<Duration> getSocketConnectTimeout()
    {
        return Optional.ofNullable(socketConnectTimeout);
    }

    @Config("s3.socket-connect-timeout")
    @ConfigDescription("Maximum time allowed for socket connect to complete before timing out")
    public S3FileSystemConfig setSocketConnectTimeout(Duration socketConnectTimeout)
    {
        this.socketConnectTimeout = socketConnectTimeout;
        return this;
    }

    public Optional<Duration> getSocketReadTimeout()
    {
        return Optional.ofNullable(socketReadTimeout);
    }

    @Config("s3.socket-read-timeout")
    @ConfigDescription("Maximum time allowed for socket reads before timing out")
    public S3FileSystemConfig setSocketReadTimeout(Duration socketReadTimeout)
    {
        this.socketReadTimeout = socketReadTimeout;
        return this;
    }

    public boolean getTcpKeepAlive()
    {
        return tcpKeepAlive;
    }

    @Config("s3.tcp-keep-alive")
    @ConfigDescription("Enable TCP keep alive on created connections")
    public S3FileSystemConfig setTcpKeepAlive(boolean tcpKeepAlive)
    {
        this.tcpKeepAlive = tcpKeepAlive;
        return this;
    }

    public HostAndPort getHttpProxy()
    {
        return httpProxy;
    }

    @Config("s3.http-proxy")
    public S3FileSystemConfig setHttpProxy(HostAndPort httpProxy)
    {
        this.httpProxy = httpProxy;
        return this;
    }

    public boolean isHttpProxySecure()
    {
        return httpProxySecure;
    }

    @Config("s3.http-proxy.secure")
    public S3FileSystemConfig setHttpProxySecure(boolean httpProxySecure)
    {
        this.httpProxySecure = httpProxySecure;
        return this;
    }
}
