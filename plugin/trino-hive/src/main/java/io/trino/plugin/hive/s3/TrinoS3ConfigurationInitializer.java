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

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.hive.ConfigurationInitializer;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import java.io.File;
import java.util.List;

import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_ACCESS_KEY;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_ACL_TYPE;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_CONNECT_TIMEOUT;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_ENCRYPTION_MATERIALS_PROVIDER;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_ENDPOINT;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_EXTERNAL_ID;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_IAM_ROLE;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_KMS_KEY_ID;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_MAX_BACKOFF_TIME;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_MAX_CLIENT_RETRIES;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_MAX_CONNECTIONS;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_MAX_ERROR_RETRIES;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_MAX_RETRY_TIME;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_MULTIPART_MIN_FILE_SIZE;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_MULTIPART_MIN_PART_SIZE;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_NON_PROXY_HOSTS;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_PATH_STYLE_ACCESS;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_PIN_CLIENT_TO_CURRENT_REGION;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_PREEMPTIVE_BASIC_PROXY_AUTH;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_PROXY_HOST;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_PROXY_PASSWORD;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_PROXY_PORT;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_PROXY_PROTOCOL;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_PROXY_USERNAME;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_REQUESTER_PAYS_ENABLED;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_SECRET_KEY;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_SIGNER_CLASS;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_SIGNER_TYPE;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_SKIP_GLACIER_OBJECTS;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_SOCKET_TIMEOUT;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_SSE_ENABLED;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_SSE_KMS_KEY_ID;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_SSE_TYPE;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_SSL_ENABLED;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_STAGING_DIRECTORY;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_STORAGE_CLASS;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_STREAMING_UPLOAD_ENABLED;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_STREAMING_UPLOAD_PART_SIZE;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_STS_ENDPOINT;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_STS_REGION;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_USER_AGENT_PREFIX;
import static java.util.stream.Collectors.joining;

public class TrinoS3ConfigurationInitializer
        implements ConfigurationInitializer
{
    private final String awsAccessKey;
    private final String awsSecretKey;
    private final String endpoint;
    private final TrinoS3StorageClass s3StorageClass;
    private final TrinoS3SignerType signerType;
    private final boolean pathStyleAccess;
    private final String iamRole;
    private final String externalId;
    private final boolean sslEnabled;
    private final boolean sseEnabled;
    private final TrinoS3SseType sseType;
    private final String encryptionMaterialsProvider;
    private final String kmsKeyId;
    private final String sseKmsKeyId;
    private final int maxClientRetries;
    private final int maxErrorRetries;
    private final Duration maxBackoffTime;
    private final Duration maxRetryTime;
    private final Duration connectTimeout;
    private final Duration socketTimeout;
    private final int maxConnections;
    private final DataSize multipartMinFileSize;
    private final DataSize multipartMinPartSize;
    private final File stagingDirectory;
    private final boolean pinClientToCurrentRegion;
    private final String userAgentPrefix;
    private final TrinoS3AclType aclType;
    private final String signerClass;
    private final boolean requesterPaysEnabled;
    private final boolean skipGlacierObjects;
    private final boolean s3StreamingUploadEnabled;
    private final DataSize streamingPartSize;
    private final String s3proxyHost;
    private final int s3proxyPort;
    private final TrinoS3Protocol s3ProxyProtocol;
    private final List<String> s3nonProxyHosts;
    private final String s3proxyUsername;
    private final String s3proxyPassword;
    private final boolean s3preemptiveBasicProxyAuth;
    private final String s3StsEndpoint;
    private final String s3StsRegion;

    @Inject
    public TrinoS3ConfigurationInitializer(HiveS3Config config)
    {
        this.awsAccessKey = config.getS3AwsAccessKey();
        this.awsSecretKey = config.getS3AwsSecretKey();
        this.endpoint = config.getS3Endpoint();
        this.s3StorageClass = config.getS3StorageClass();
        this.signerType = config.getS3SignerType();
        this.signerClass = config.getS3SignerClass();
        this.pathStyleAccess = config.isS3PathStyleAccess();
        this.iamRole = config.getS3IamRole();
        this.externalId = config.getS3ExternalId();
        this.sslEnabled = config.isS3SslEnabled();
        this.sseEnabled = config.isS3SseEnabled();
        this.sseType = config.getS3SseType();
        this.encryptionMaterialsProvider = config.getS3EncryptionMaterialsProvider();
        this.kmsKeyId = config.getS3KmsKeyId();
        this.sseKmsKeyId = config.getS3SseKmsKeyId();
        this.maxClientRetries = config.getS3MaxClientRetries();
        this.maxErrorRetries = config.getS3MaxErrorRetries();
        this.maxBackoffTime = config.getS3MaxBackoffTime();
        this.maxRetryTime = config.getS3MaxRetryTime();
        this.connectTimeout = config.getS3ConnectTimeout();
        this.socketTimeout = config.getS3SocketTimeout();
        this.maxConnections = config.getS3MaxConnections();
        this.multipartMinFileSize = config.getS3MultipartMinFileSize();
        this.multipartMinPartSize = config.getS3MultipartMinPartSize();
        this.stagingDirectory = config.getS3StagingDirectory();
        this.pinClientToCurrentRegion = config.isPinS3ClientToCurrentRegion();
        this.userAgentPrefix = config.getS3UserAgentPrefix();
        this.aclType = config.getS3AclType();
        this.skipGlacierObjects = config.isSkipGlacierObjects();
        this.requesterPaysEnabled = config.isRequesterPaysEnabled();
        this.s3StreamingUploadEnabled = config.isS3StreamingUploadEnabled();
        this.streamingPartSize = config.getS3StreamingPartSize();
        this.s3proxyHost = config.getS3ProxyHost();
        this.s3proxyPort = config.getS3ProxyPort();
        this.s3ProxyProtocol = config.getS3ProxyProtocol();
        this.s3nonProxyHosts = config.getS3NonProxyHosts();
        this.s3proxyUsername = config.getS3ProxyUsername();
        this.s3proxyPassword = config.getS3ProxyPassword();
        this.s3preemptiveBasicProxyAuth = config.getS3PreemptiveBasicProxyAuth();
        this.s3StsEndpoint = config.getS3StsEndpoint();
        this.s3StsRegion = config.getS3StsRegion();
    }

    @Override
    public void initializeConfiguration(Configuration config)
    {
        // re-map filesystem schemes to match Amazon Elastic MapReduce
        config.set("fs.s3.impl", TrinoS3FileSystem.class.getName());
        config.set("fs.s3a.impl", TrinoS3FileSystem.class.getName());
        config.set("fs.s3n.impl", TrinoS3FileSystem.class.getName());

        if (awsAccessKey != null) {
            config.set(S3_ACCESS_KEY, awsAccessKey);
        }
        if (awsSecretKey != null) {
            config.set(S3_SECRET_KEY, awsSecretKey);
        }
        if (endpoint != null) {
            config.set(S3_ENDPOINT, endpoint);
        }
        config.set(S3_STORAGE_CLASS, s3StorageClass.name());
        if (signerType != null) {
            config.set(S3_SIGNER_TYPE, signerType.name());
        }
        if (signerClass != null) {
            config.set(S3_SIGNER_CLASS, signerClass);
        }
        config.setBoolean(S3_PATH_STYLE_ACCESS, pathStyleAccess);
        if (iamRole != null) {
            config.set(S3_IAM_ROLE, iamRole);
        }
        if (externalId != null) {
            config.set(S3_EXTERNAL_ID, externalId);
        }
        config.setBoolean(S3_SSL_ENABLED, sslEnabled);
        config.setBoolean(S3_SSE_ENABLED, sseEnabled);
        config.set(S3_SSE_TYPE, sseType.name());
        if (encryptionMaterialsProvider != null) {
            config.set(S3_ENCRYPTION_MATERIALS_PROVIDER, encryptionMaterialsProvider);
        }
        if (kmsKeyId != null) {
            config.set(S3_KMS_KEY_ID, kmsKeyId);
        }
        if (sseKmsKeyId != null) {
            config.set(S3_SSE_KMS_KEY_ID, sseKmsKeyId);
        }
        config.setInt(S3_MAX_CLIENT_RETRIES, maxClientRetries);
        config.setInt(S3_MAX_ERROR_RETRIES, maxErrorRetries);
        config.set(S3_MAX_BACKOFF_TIME, maxBackoffTime.toString());
        config.set(S3_MAX_RETRY_TIME, maxRetryTime.toString());
        config.set(S3_CONNECT_TIMEOUT, connectTimeout.toString());
        config.set(S3_SOCKET_TIMEOUT, socketTimeout.toString());
        config.set(S3_STAGING_DIRECTORY, stagingDirectory.getPath());
        config.setInt(S3_MAX_CONNECTIONS, maxConnections);
        config.setLong(S3_MULTIPART_MIN_FILE_SIZE, multipartMinFileSize.toBytes());
        config.setLong(S3_MULTIPART_MIN_PART_SIZE, multipartMinPartSize.toBytes());
        config.setBoolean(S3_PIN_CLIENT_TO_CURRENT_REGION, pinClientToCurrentRegion);
        config.set(S3_USER_AGENT_PREFIX, userAgentPrefix);
        config.set(S3_ACL_TYPE, aclType.name());
        config.setBoolean(S3_SKIP_GLACIER_OBJECTS, skipGlacierObjects);
        config.setBoolean(S3_REQUESTER_PAYS_ENABLED, requesterPaysEnabled);
        config.setBoolean(S3_STREAMING_UPLOAD_ENABLED, s3StreamingUploadEnabled);
        config.setLong(S3_STREAMING_UPLOAD_PART_SIZE, streamingPartSize.toBytes());
        if (s3proxyHost != null) {
            config.set(S3_PROXY_HOST, s3proxyHost);
        }
        if (s3proxyPort > -1) {
            config.setInt(S3_PROXY_PORT, s3proxyPort);
        }
        if (s3ProxyProtocol != null) {
            config.set(S3_PROXY_PROTOCOL, s3ProxyProtocol.name());
        }
        if (s3nonProxyHosts != null) {
            config.set(S3_NON_PROXY_HOSTS, s3nonProxyHosts.stream().collect(joining("|")));
        }
        if (s3proxyUsername != null) {
            config.set(S3_PROXY_USERNAME, s3proxyUsername);
        }
        if (s3proxyPassword != null) {
            config.set(S3_PROXY_PASSWORD, s3proxyPassword);
        }
        config.setBoolean(S3_PREEMPTIVE_BASIC_PROXY_AUTH, s3preemptiveBasicProxyAuth);
        if (s3StsEndpoint != null) {
            config.set(S3_STS_ENDPOINT, s3StsEndpoint);
        }
        if (s3StsRegion != null) {
            config.set(S3_STS_REGION, s3StsRegion);
        }
    }
}
