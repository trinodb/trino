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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.hdfs.s3.HiveS3Config;
import io.trino.hdfs.s3.TrinoS3FileSystem;
import io.trino.plugin.hive.HiveConfig;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.net.URI;
import java.util.Optional;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_ACCESS_KEY;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_CONNECT_TIMEOUT;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_CONNECT_TTL;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_CREDENTIALS_PROVIDER;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_ENDPOINT;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_EXTERNAL_ID;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_IAM_ROLE;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_MAX_ERROR_RETRIES;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_PIN_CLIENT_TO_CURRENT_REGION;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_REGION;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_ROLE_SESSION_NAME;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_SECRET_KEY;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_SESSION_TOKEN;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_SOCKET_TIMEOUT;
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

    private S3SelectStats stats;

    @GuardedBy("this")
    private S3AsyncClient s3Client;

    @Inject
    public TrinoS3ClientFactory(HiveConfig config, S3SelectStats stats)
    {
        this.enabled = config.isS3SelectPushdownEnabled();
        this.defaultMaxConnections = config.getS3SelectPushdownMaxConnections();
        this.stats = stats;
    }

    synchronized S3AsyncClient getS3Client(Configuration config)
    {
        if (s3Client == null) {
            s3Client = createS3Client(config);
        }
        return s3Client;
    }

    private ClientOverrideConfiguration getClientOverrideConfig(RetryPolicy retryPolicy, String userAgentPrefix)
    {
        return ClientOverrideConfiguration.builder()
                .putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, userAgentPrefix)
                .putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_SUFFIX, enabled ? "Trino-select" : "Trino")
                .addMetricPublisher(stats.newRequestMetricCollector())
                .retryPolicy(retryPolicy).build();
    }

    private S3AsyncClient createS3Client(Configuration config)
    {
        HiveS3Config defaults = new HiveS3Config();
        String userAgentPrefix = config.get(S3_USER_AGENT_PREFIX, defaults.getS3UserAgentPrefix());
        int maxErrorRetries = config.getInt(S3_MAX_ERROR_RETRIES, defaults.getS3MaxErrorRetries());
        Duration connectTimeout = Duration.valueOf(config.get(S3_CONNECT_TIMEOUT, defaults.getS3ConnectTimeout().toString()));
        Duration socketTimeout = Duration.valueOf(config.get(S3_SOCKET_TIMEOUT, defaults.getS3SocketTimeout().toString()));
        int maxConnections = config.getInt(S3_SELECT_PUSHDOWN_MAX_CONNECTIONS, defaultMaxConnections);

        NettyNioAsyncHttpClient.Builder nettyBuilder = NettyNioAsyncHttpClient.builder()
                .maxConcurrency(maxConnections)
                .connectionTimeout(java.time.Duration.ofMillis(toIntExact(connectTimeout.toMillis())))
                .readTimeout(java.time.Duration.ofMillis(toIntExact(socketTimeout.toMillis())))
                .writeTimeout(java.time.Duration.ofMillis(toIntExact(socketTimeout.toMillis())));

        RetryPolicy retryPolicy = RetryPolicy.builder().numRetries(maxErrorRetries).build();
        ClientOverrideConfiguration clientOverrideConfiguration = getClientOverrideConfig(retryPolicy, userAgentPrefix);
        S3Configuration s3Configuration = S3Configuration.builder().pathStyleAccessEnabled(true).build();

        String connectTtlValue = config.get(S3_CONNECT_TTL);
        if (!isNullOrEmpty(connectTtlValue)) {
            nettyBuilder.connectionTimeToLive(java.time.Duration.ofMillis(Duration.valueOf(connectTtlValue).toMillis()));
        }

        AwsCredentialsProvider awsCredentialsProvider = getAwsCredentialsProvider(config);
        S3AsyncClientBuilder clientBuilder = S3AsyncClient.builder()
                .httpClientBuilder(nettyBuilder)
                .credentialsProvider(awsCredentialsProvider)
                .overrideConfiguration(clientOverrideConfiguration)
                .serviceConfiguration(s3Configuration);

        boolean regionOrEndpointSet = false;

        String endpoint = config.get(S3_ENDPOINT);
        String region = config.get(S3_REGION);
        boolean pinS3ClientToCurrentRegion = config.getBoolean(S3_PIN_CLIENT_TO_CURRENT_REGION, defaults.isPinS3ClientToCurrentRegion());
        verify(!pinS3ClientToCurrentRegion || endpoint == null,
                "Invalid configuration: either endpoint can be set or S3 client can be pinned to the current region");

        // use local region when running inside of EC2
        if (pinS3ClientToCurrentRegion) {
            regionOrEndpointSet = true;
        }

        if (!isNullOrEmpty(endpoint)) {
            clientBuilder.endpointOverride(URI.create(endpoint));
            regionOrEndpointSet = true;
        }
        else if (!isNullOrEmpty(region)) {
            clientBuilder.region(Region.of(region));
            regionOrEndpointSet = true;
        }

        if (!regionOrEndpointSet) {
            clientBuilder.region(Region.US_EAST_1);
            clientBuilder.crossRegionAccessEnabled(true);
        }

        return clientBuilder.build();
    }

    private static AwsCredentialsProvider getAwsCredentialsProvider(Configuration conf)
    {
        Optional<AwsCredentials> credentials = getAwsCredentials(conf);
        if (credentials.isPresent()) {
            return StaticCredentialsProvider.create(credentials.get());
        }

        String providerClass = conf.get(S3_CREDENTIALS_PROVIDER);
        if (!isNullOrEmpty(providerClass)) {
            return getCustomAWSCredentialsProvider(conf, providerClass);
        }

        AwsCredentialsProvider provider = getAwsCredentials(conf)
                .map(value -> (AwsCredentialsProvider) StaticCredentialsProvider.create(value))
                .orElseGet(DefaultCredentialsProvider::create);

        String iamRole = conf.get(S3_IAM_ROLE);
        if (iamRole != null) {
            String stsEndpointOverride = conf.get(S3_STS_ENDPOINT);
            String stsRegionOverride = conf.get(S3_STS_REGION);
            String s3RoleSessionName = conf.get(S3_ROLE_SESSION_NAME);
            String externalId = conf.get(S3_EXTERNAL_ID);

            StsClientBuilder stsClientBuilder = StsClient.builder().credentialsProvider(provider);

            Region region;
            if (!isNullOrEmpty(stsRegionOverride)) {
                region = Region.of(stsRegionOverride);
            }
            else {
                DefaultAwsRegionProviderChain regionProviderChain = DefaultAwsRegionProviderChain.builder().build();
                try {
                    region = regionProviderChain.getRegion();
                }
                catch (SdkClientException ex) {
                    log.warn("Falling back to default AWS region %s", Region.US_EAST_1.id());
                    region = Region.US_EAST_1;
                }
            }

            if (!isNullOrEmpty(stsEndpointOverride)) {
                stsClientBuilder.endpointOverride(URI.create(stsEndpointOverride));
            }
            else {
                stsClientBuilder.region(region);
            }

            AssumeRoleRequest roleRequest = AssumeRoleRequest.builder()
                    .roleArn(iamRole)
                    .roleSessionName(s3RoleSessionName)
                    .externalId(externalId)
                    .build();

            provider = StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClientBuilder.build())
                    .refreshRequest(() -> roleRequest)
                    .build();
        }

        return provider;
    }

    private static AwsCredentialsProvider getCustomAWSCredentialsProvider(Configuration conf, String providerClass)
    {
        try {
            return conf.getClassByName(providerClass)
                    .asSubclass(AwsCredentialsProvider.class)
                    .getConstructor(URI.class, Configuration.class)
                    .newInstance(null, conf);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(format("Error creating an instance of %s", providerClass), e);
        }
    }

    private static Optional<AwsCredentials> getAwsCredentials(Configuration conf)
    {
        String accessKey = conf.get(S3_ACCESS_KEY);
        String secretKey = conf.get(S3_SECRET_KEY);

        if (isNullOrEmpty(accessKey) || isNullOrEmpty(secretKey)) {
            return Optional.empty();
        }
        String sessionToken = conf.get(S3_SESSION_TOKEN);
        if (!isNullOrEmpty(sessionToken)) {
            return Optional.of(AwsSessionCredentials.create(accessKey, secretKey, sessionToken));
        }

        return Optional.of(AwsBasicCredentials.create(accessKey, secretKey));
    }
}
