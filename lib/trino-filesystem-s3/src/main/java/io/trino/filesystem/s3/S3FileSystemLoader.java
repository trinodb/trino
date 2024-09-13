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

import com.google.inject.Inject;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.awssdk.v2_2.AwsSdkTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import jakarta.annotation.PreDestroy;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.filesystem.s3.S3FileSystemConfig.RetryMode.getRetryStrategy;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

final class S3FileSystemLoader
        implements Function<Location, TrinoFileSystemFactory>
{
    private final Optional<S3SecurityMappingProvider> mappingProvider;
    private final SdkHttpClient httpClient;
    private final S3ClientFactory clientFactory;
    private final S3Presigner preSigner;
    private final S3Context context;
    private final ExecutorService uploadExecutor = newCachedThreadPool(daemonThreadsNamed("s3-upload-%s"));

    @Inject
    public S3FileSystemLoader(S3SecurityMappingProvider mappingProvider, OpenTelemetry openTelemetry, S3FileSystemConfig config, S3FileSystemStats stats)
    {
        this(Optional.of(mappingProvider), openTelemetry, config, stats);
    }

    S3FileSystemLoader(OpenTelemetry openTelemetry, S3FileSystemConfig config, S3FileSystemStats stats)
    {
        this(Optional.empty(), openTelemetry, config, stats);
    }

    private S3FileSystemLoader(Optional<S3SecurityMappingProvider> mappingProvider, OpenTelemetry openTelemetry, S3FileSystemConfig config, S3FileSystemStats stats)
    {
        this.mappingProvider = requireNonNull(mappingProvider, "mappingProvider is null");
        this.httpClient = createHttpClient(config);

        requireNonNull(stats, "stats is null");

        MetricPublisher metricPublisher = stats.newMetricPublisher();
        this.clientFactory = s3ClientFactory(httpClient, openTelemetry, config, metricPublisher);

        this.preSigner = s3PreSigner(httpClient, openTelemetry, config, metricPublisher);

        this.context = new S3Context(
                toIntExact(config.getStreamingPartSize().toBytes()),
                config.isRequesterPays(),
                config.getSseType(),
                config.getSseKmsKeyId(),
                Optional.empty(),
                config.getCannedAcl(),
                config.isSupportsExclusiveCreate());
    }

    @Override
    public TrinoFileSystemFactory apply(Location location)
    {
        return new S3SecurityMappingFileSystemFactory(mappingProvider.orElseThrow(), clientFactory, preSigner, context, location, uploadExecutor);
    }

    @PreDestroy
    public void destroy()
    {
        try (httpClient) {
            uploadExecutor.shutdownNow();
        }
    }

    S3Client createClient()
    {
        return clientFactory.create(Optional.empty());
    }

    S3Presigner createPreSigner()
    {
        return preSigner;
    }

    S3Context context()
    {
        return context;
    }

    Executor uploadExecutor()
    {
        return uploadExecutor;
    }

    private static S3ClientFactory s3ClientFactory(SdkHttpClient httpClient, OpenTelemetry openTelemetry, S3FileSystemConfig config, MetricPublisher metricPublisher)
    {
        ClientOverrideConfiguration overrideConfiguration = createOverrideConfiguration(openTelemetry, config, metricPublisher);

        Optional<AwsCredentialsProvider> staticCredentialsProvider = createStaticCredentialsProvider(config);
        Optional<String> staticRegion = Optional.ofNullable(config.getRegion());
        Optional<String> staticEndpoint = Optional.ofNullable(config.getEndpoint());
        boolean pathStyleAccess = config.isPathStyleAccess();
        boolean useWebIdentityTokenCredentialsProvider = config.isUseWebIdentityTokenCredentialsProvider();
        Optional<String> staticIamRole = Optional.ofNullable(config.getIamRole());
        String staticRoleSessionName = config.getRoleSessionName();
        String externalId = config.getExternalId();

        return mapping -> {
            Optional<AwsCredentialsProvider> credentialsProvider = mapping
                    .flatMap(S3SecurityMappingResult::credentialsProvider)
                    .or(() -> staticCredentialsProvider);

            Optional<String> region = mapping.flatMap(S3SecurityMappingResult::region).or(() -> staticRegion);
            Optional<String> endpoint = mapping.flatMap(S3SecurityMappingResult::endpoint).or(() -> staticEndpoint);

            Optional<String> iamRole = mapping.flatMap(S3SecurityMappingResult::iamRole).or(() -> staticIamRole);
            String roleSessionName = mapping.flatMap(S3SecurityMappingResult::roleSessionName).orElse(staticRoleSessionName);

            S3ClientBuilder s3 = S3Client.builder();
            s3.overrideConfiguration(overrideConfiguration);
            s3.httpClient(httpClient);

            region.map(Region::of).ifPresent(s3::region);
            endpoint.map(URI::create).ifPresent(s3::endpointOverride);
            s3.forcePathStyle(pathStyleAccess);

            if (useWebIdentityTokenCredentialsProvider) {
                s3.credentialsProvider(WebIdentityTokenFileCredentialsProvider.builder()
                        .asyncCredentialUpdateEnabled(true)
                        .build());
            }
            else if (iamRole.isPresent()) {
                s3.credentialsProvider(StsAssumeRoleCredentialsProvider.builder()
                        .refreshRequest(request -> request
                                .roleArn(iamRole.get())
                                .roleSessionName(roleSessionName)
                                .externalId(externalId))
                        .stsClient(createStsClient(config, credentialsProvider))
                        .asyncCredentialUpdateEnabled(true)
                        .build());
            }
            else {
                credentialsProvider.ifPresent(s3::credentialsProvider);
            }

            return s3.build();
        };
    }

    private static S3Presigner s3PreSigner(SdkHttpClient httpClient, OpenTelemetry openTelemetry, S3FileSystemConfig config, MetricPublisher metricPublisher)
    {
        Optional<AwsCredentialsProvider> staticCredentialsProvider = createStaticCredentialsProvider(config);
        Optional<String> staticRegion = Optional.ofNullable(config.getRegion());
        Optional<String> staticEndpoint = Optional.ofNullable(config.getEndpoint());
        boolean pathStyleAccess = config.isPathStyleAccess();
        boolean useWebIdentityTokenCredentialsProvider = config.isUseWebIdentityTokenCredentialsProvider();
        Optional<String> staticIamRole = Optional.ofNullable(config.getIamRole());
        String staticRoleSessionName = config.getRoleSessionName();
        String externalId = config.getExternalId();

        S3Presigner.Builder s3 = S3Presigner.builder();
        s3.s3Client(s3ClientFactory(httpClient, openTelemetry, config, metricPublisher)
                .create(Optional.empty()));

        staticRegion.map(Region::of).ifPresent(s3::region);
        staticEndpoint.map(URI::create).ifPresent(s3::endpointOverride);
        s3.serviceConfiguration(S3Configuration.builder()
                .pathStyleAccessEnabled(pathStyleAccess)
                .build());

        if (useWebIdentityTokenCredentialsProvider) {
            s3.credentialsProvider(WebIdentityTokenFileCredentialsProvider.builder()
                    .asyncCredentialUpdateEnabled(true)
                    .build());
        }
        else if (staticIamRole.isPresent()) {
            s3.credentialsProvider(StsAssumeRoleCredentialsProvider.builder()
                    .refreshRequest(request -> request
                            .roleArn(staticIamRole.get())
                            .roleSessionName(staticRoleSessionName)
                            .externalId(externalId))
                    .stsClient(createStsClient(config, staticCredentialsProvider))
                    .asyncCredentialUpdateEnabled(true)
                    .build());
        }
        else {
            staticCredentialsProvider.ifPresent(s3::credentialsProvider);
        }

        return s3.build();
    }

    private static Optional<AwsCredentialsProvider> createStaticCredentialsProvider(S3FileSystemConfig config)
    {
        if ((config.getAwsAccessKey() != null) || (config.getAwsSecretKey() != null)) {
            return Optional.of(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.getAwsAccessKey(), config.getAwsSecretKey())));
        }
        return Optional.empty();
    }

    private static StsClient createStsClient(S3FileSystemConfig config, Optional<AwsCredentialsProvider> credentialsProvider)
    {
        StsClientBuilder sts = StsClient.builder();
        Optional.ofNullable(config.getStsEndpoint()).map(URI::create).ifPresent(sts::endpointOverride);
        Optional.ofNullable(config.getStsRegion())
                .or(() -> Optional.ofNullable(config.getRegion()))
                .map(Region::of).ifPresent(sts::region);
        credentialsProvider.ifPresent(sts::credentialsProvider);
        return sts.build();
    }

    private static ClientOverrideConfiguration createOverrideConfiguration(OpenTelemetry openTelemetry, S3FileSystemConfig config, MetricPublisher metricPublisher)
    {
        return ClientOverrideConfiguration.builder()
                .addExecutionInterceptor(AwsSdkTelemetry.builder(openTelemetry)
                        .setCaptureExperimentalSpanAttributes(true)
                        .setRecordIndividualHttpError(true)
                        .build().newExecutionInterceptor())
                .retryStrategy(getRetryStrategy(config.getRetryMode()).toBuilder()
                        .maxAttempts(config.getMaxErrorRetries())
                        .build())
                .addMetricPublisher(metricPublisher)
                .build();
    }

    private static SdkHttpClient createHttpClient(S3FileSystemConfig config)
    {
        ApacheHttpClient.Builder client = ApacheHttpClient.builder()
                .maxConnections(config.getMaxConnections())
                .tcpKeepAlive(config.getTcpKeepAlive());

        config.getConnectionTtl().ifPresent(ttl -> client.connectionTimeToLive(ttl.toJavaTime()));
        config.getConnectionMaxIdleTime().ifPresent(time -> client.connectionMaxIdleTime(time.toJavaTime()));
        config.getSocketConnectTimeout().ifPresent(timeout -> client.connectionTimeout(timeout.toJavaTime()));
        config.getSocketReadTimeout().ifPresent(timeout -> client.socketTimeout(timeout.toJavaTime()));

        if (config.getHttpProxy() != null) {
            client.proxyConfiguration(ProxyConfiguration.builder()
                    .endpoint(URI.create("%s://%s".formatted(
                            config.isHttpProxySecure() ? "https" : "http",
                            config.getHttpProxy())))
                    .username(config.getHttpProxyUsername())
                    .password(config.getHttpProxyPassword())
                    .nonProxyHosts(config.getNonProxyHosts())
                    .preemptiveBasicAuthenticationEnabled(config.getHttpProxyPreemptiveBasicProxyAuth())
                    .build());
        }

        return client.build();
    }

    interface S3ClientFactory
    {
        S3Client create(Optional<S3SecurityMappingResult> mapping);
    }
}
