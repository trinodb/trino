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
package io.trino.plugin.hive.s3select;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Builder;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.hdfs.s3.HiveS3Config;
import io.trino.hdfs.s3.TrinoS3FileSystem;
import io.trino.plugin.hive.HiveConfig;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.util.Optional;

import static com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import static com.amazonaws.regions.Regions.US_EAST_1;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static io.trino.hdfs.s3.AwsCurrentRegionHolder.getCurrentRegionFromEC2Metadata;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_ACCESS_KEY;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_CONNECT_TIMEOUT;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_CONNECT_TTL;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_CREDENTIALS_PROVIDER;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_ENDPOINT;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_EXTERNAL_ID;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_IAM_ROLE;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_MAX_ERROR_RETRIES;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_PIN_CLIENT_TO_CURRENT_REGION;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_ROLE_SESSION_NAME;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_SECRET_KEY;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_SESSION_TOKEN;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_SOCKET_TIMEOUT;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_SSL_ENABLED;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_STS_ENDPOINT;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_STS_REGION;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_USER_AGENT_PREFIX;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

/**
 * This factory provides AmazonS3 client required for executing S3SelectPushdown requests.
 * Normal S3 GET requests use AmazonS3 clients initialized in {@link TrinoS3FileSystem} or EMRFS.
 * The ideal state will be to merge this logic with the two file systems and get rid of this
 * factory class.
 * Please do not use the client provided by this factory for any other use cases.
 */
public class TrinoS3ClientFactory
{
    private static final Logger log = Logger.get(TrinoS3ClientFactory.class);
    private static final String S3_SELECT_PUSHDOWN_MAX_CONNECTIONS = "hive.s3select-pushdown.max-connections";

    private final boolean enabled;
    private final int defaultMaxConnections;

    @GuardedBy("this")
    private AmazonS3 s3Client;

    @Inject
    public TrinoS3ClientFactory(HiveConfig config)
    {
        this.enabled = config.isS3SelectPushdownEnabled();
        this.defaultMaxConnections = config.getS3SelectPushdownMaxConnections();
    }

    synchronized AmazonS3 getS3Client(Configuration config)
    {
        if (s3Client == null) {
            s3Client = createS3Client(config);
        }
        return s3Client;
    }

    private AmazonS3 createS3Client(Configuration config)
    {
        HiveS3Config defaults = new HiveS3Config();
        String userAgentPrefix = config.get(S3_USER_AGENT_PREFIX, defaults.getS3UserAgentPrefix());
        int maxErrorRetries = config.getInt(S3_MAX_ERROR_RETRIES, defaults.getS3MaxErrorRetries());
        boolean sslEnabled = config.getBoolean(S3_SSL_ENABLED, defaults.isS3SslEnabled());
        Duration connectTimeout = Duration.valueOf(config.get(S3_CONNECT_TIMEOUT, defaults.getS3ConnectTimeout().toString()));
        Duration socketTimeout = Duration.valueOf(config.get(S3_SOCKET_TIMEOUT, defaults.getS3SocketTimeout().toString()));
        int maxConnections = config.getInt(S3_SELECT_PUSHDOWN_MAX_CONNECTIONS, defaultMaxConnections);

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withMaxErrorRetry(maxErrorRetries)
                .withProtocol(sslEnabled ? Protocol.HTTPS : Protocol.HTTP)
                .withConnectionTimeout(toIntExact(connectTimeout.toMillis()))
                .withSocketTimeout(toIntExact(socketTimeout.toMillis()))
                .withMaxConnections(maxConnections)
                .withUserAgentPrefix(userAgentPrefix)
                .withUserAgentSuffix(enabled ? "Trino-select" : "Trino");

        String connectTtlValue = config.get(S3_CONNECT_TTL);
        if (!isNullOrEmpty(connectTtlValue)) {
            clientConfiguration.setConnectionTTL(Duration.valueOf(connectTtlValue).toMillis());
        }

        AWSCredentialsProvider awsCredentialsProvider = getAwsCredentialsProvider(config);
        AmazonS3Builder<? extends AmazonS3Builder<?, ?>, ? extends AmazonS3> clientBuilder = AmazonS3Client.builder()
                .withCredentials(awsCredentialsProvider)
                .withClientConfiguration(clientConfiguration)
                .withMetricsCollector(TrinoS3FileSystem.getFileSystemStats().newRequestMetricCollector())
                .enablePathStyleAccess();

        boolean regionOrEndpointSet = false;

        String endpoint = config.get(S3_ENDPOINT);
        boolean pinS3ClientToCurrentRegion = config.getBoolean(S3_PIN_CLIENT_TO_CURRENT_REGION, defaults.isPinS3ClientToCurrentRegion());
        verify(!pinS3ClientToCurrentRegion || endpoint == null,
                "Invalid configuration: either endpoint can be set or S3 client can be pinned to the current region");

        // use local region when running inside of EC2
        if (pinS3ClientToCurrentRegion) {
            clientBuilder.setRegion(getCurrentRegionFromEC2Metadata().getName());
            regionOrEndpointSet = true;
        }

        if (!isNullOrEmpty(endpoint)) {
            clientBuilder.withEndpointConfiguration(new EndpointConfiguration(endpoint, null));
            regionOrEndpointSet = true;
        }

        if (!regionOrEndpointSet) {
            clientBuilder.withRegion(US_EAST_1);
            clientBuilder.setForceGlobalBucketAccessEnabled(true);
        }

        return clientBuilder.build();
    }

    private static AWSCredentialsProvider getAwsCredentialsProvider(Configuration conf)
    {
        Optional<AWSCredentials> credentials = getAwsCredentials(conf);
        if (credentials.isPresent()) {
            return new AWSStaticCredentialsProvider(credentials.get());
        }

        String providerClass = conf.get(S3_CREDENTIALS_PROVIDER);
        if (!isNullOrEmpty(providerClass)) {
            return getCustomAWSCredentialsProvider(conf, providerClass);
        }

        AWSCredentialsProvider provider = getAwsCredentials(conf)
                .map(value -> (AWSCredentialsProvider) new AWSStaticCredentialsProvider(value))
                .orElseGet(DefaultAWSCredentialsProviderChain::getInstance);

        String iamRole = conf.get(S3_IAM_ROLE);
        if (iamRole != null) {
            String stsEndpointOverride = conf.get(S3_STS_ENDPOINT);
            String stsRegionOverride = conf.get(S3_STS_REGION);
            String s3RoleSessionName = conf.get(S3_ROLE_SESSION_NAME);
            String externalId = conf.get(S3_EXTERNAL_ID);

            AWSSecurityTokenServiceClientBuilder stsClientBuilder = AWSSecurityTokenServiceClientBuilder.standard()
                    .withCredentials(provider);

            String region;
            if (!isNullOrEmpty(stsRegionOverride)) {
                region = stsRegionOverride;
            }
            else {
                DefaultAwsRegionProviderChain regionProviderChain = new DefaultAwsRegionProviderChain();
                try {
                    region = regionProviderChain.getRegion();
                }
                catch (SdkClientException ex) {
                    log.warn("Falling back to default AWS region %s", US_EAST_1);
                    region = US_EAST_1.getName();
                }
            }

            if (!isNullOrEmpty(stsEndpointOverride)) {
                stsClientBuilder.withEndpointConfiguration(new EndpointConfiguration(stsEndpointOverride, region));
            }
            else {
                stsClientBuilder.withRegion(region);
            }

            provider = new STSAssumeRoleSessionCredentialsProvider.Builder(iamRole, s3RoleSessionName)
                    .withExternalId(externalId)
                    .withStsClient(stsClientBuilder.build())
                    .build();
        }

        return provider;
    }

    private static AWSCredentialsProvider getCustomAWSCredentialsProvider(Configuration conf, String providerClass)
    {
        try {
            return conf.getClassByName(providerClass)
                    .asSubclass(AWSCredentialsProvider.class)
                    .getConstructor(URI.class, Configuration.class)
                    .newInstance(null, conf);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(format("Error creating an instance of %s", providerClass), e);
        }
    }

    private static Optional<AWSCredentials> getAwsCredentials(Configuration conf)
    {
        String accessKey = conf.get(S3_ACCESS_KEY);
        String secretKey = conf.get(S3_SECRET_KEY);

        if (isNullOrEmpty(accessKey) || isNullOrEmpty(secretKey)) {
            return Optional.empty();
        }
        String sessionToken = conf.get(S3_SESSION_TOKEN);
        if (!isNullOrEmpty(sessionToken)) {
            return Optional.of(new BasicSessionCredentials(accessKey, secretKey, sessionToken));
        }

        return Optional.of(new BasicAWSCredentials(accessKey, secretKey));
    }
}
