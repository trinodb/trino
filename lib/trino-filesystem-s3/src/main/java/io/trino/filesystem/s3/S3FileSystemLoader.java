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

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.awssdk.v2_2.AwsSdkTelemetry;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.S3Context.S3SseContext;
import io.trino.filesystem.s3.S3FileSystemConfig.S3AuthType;
import io.trino.filesystem.s3.S3FileSystemConfig.SignerType;
import jakarta.annotation.PreDestroy;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.http.auth.aws.scheme.AwsV4AuthScheme;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.scheme.AuthSchemeOption;
import software.amazon.awssdk.http.auth.spi.signer.SignerProperty;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.LegacyMd5Plugin;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeProvider;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.filesystem.s3.S3FileSystemConfig.RetryMode.getRetryStrategy;
import static io.trino.filesystem.s3.S3FileSystemUtils.createS3PreSigner;
import static io.trino.filesystem.s3.S3FileSystemUtils.createStaticCredentialsProvider;
import static io.trino.filesystem.s3.S3FileSystemUtils.createStsClient;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static software.amazon.awssdk.core.checksums.ResponseChecksumValidation.WHEN_REQUIRED;

final class S3FileSystemLoader
        implements Function<Location, TrinoFileSystemFactory>
{
    private static final Logger log = Logger.get(S3FileSystemLoader.class);
    private static final long MAX_CLIENT_RESOURCES_CACHE_SIZE = 512;

    private final Optional<S3SecurityMappingProvider> mappingProvider;
    private final SdkHttpClient httpClient;
    private final S3ClientFactory clientFactory;
    private final S3FileSystemConfig config;
    private final S3Context context;
    private final ExecutorService uploadExecutor = newCachedThreadPool(daemonThreadsNamed("s3-upload-%s"));
    private final Cache<Optional<S3SecurityMappingResult>, S3ClientResources> clientResources;
    private final TrinoFileSystemFactory defaultFactory;

    @Inject
    public S3FileSystemLoader(Optional<S3SecurityMappingProvider> mappingProvider, OpenTelemetry openTelemetry, S3FileSystemConfig config, S3FileSystemStats stats)
    {
        this.mappingProvider = requireNonNull(mappingProvider, "mappingProvider is null");
        this.httpClient = createHttpClient(config);

        requireNonNull(stats, "stats is null");

        MetricPublisher metricPublisher = stats.newMetricPublisher();
        this.clientFactory = s3ClientFactory(httpClient, openTelemetry, config, metricPublisher);
        this.config = requireNonNull(config, "config is null");
        this.context = new S3Context(
                toIntExact(config.getStreamingPartSize().toBytes()),
                config.isRequesterPays(),
                S3SseContext.of(
                        config.getSseType(),
                        config.getSseKmsKeyId(),
                        config.getSseCustomerKey()),
                Optional.empty(),
                config.getStorageClass(),
                config.getCannedAcl());
        this.clientResources = EvictableCacheBuilder.newBuilder()
                .maximumSize(MAX_CLIENT_RESOURCES_CACHE_SIZE)
                .build();
        this.defaultFactory = identity -> {
            S3ClientResources resources = getOrCreateClientResources(Optional.empty());
            return new S3FileSystem(uploadExecutor, resources.client(), resources.preSigner(), context.withCredentials(identity));
        };
    }

    S3FileSystemLoader(OpenTelemetry openTelemetry, S3FileSystemConfig config, S3FileSystemStats stats)
    {
        this(Optional.empty(), openTelemetry, config, stats);
    }

    @Override
    public TrinoFileSystemFactory apply(Location location)
    {
        if (mappingProvider.isEmpty()) {
            return defaultFactory;
        }
        return identity -> {
            Optional<S3SecurityMappingResult> mapping = mappingProvider
                    .flatMap(provider -> provider.getMapping(identity, location));

            S3ClientResources resources = getOrCreateClientResources(mapping);
            S3Context context = this.context.withCredentials(identity);

            if (mapping.isPresent() && mapping.get().kmsKeyId().isPresent()) {
                checkState(mapping.get().sseCustomerKey().isEmpty(), "Both SSE-C and KMS-managed keys cannot be used at the same time");
                context = context.withKmsKeyId(mapping.get().kmsKeyId().get());
            }

            if (mapping.isPresent() && mapping.get().sseCustomerKey().isPresent()) {
                context = context.withSseCustomerKey(mapping.get().sseCustomerKey().get());
            }

            return new S3FileSystem(uploadExecutor, resources.client(), resources.preSigner(), context);
        };
    }

    @PreDestroy
    public void destroy()
    {
        try {
            for (S3ClientResources resources : clientResources.asMap().values()) {
                closeClientResources(resources);
            }
            clientResources.invalidateAll();
            clientResources.cleanUp();
        }
        finally {
            try (httpClient) {
                uploadExecutor.shutdownNow();
            }
        }
    }

    private S3ClientResources getOrCreateClientResources(Optional<S3SecurityMappingResult> mapping)
    {
        try {
            return clientResources.get(mapping, () -> {
                S3Client client = clientFactory.create(mapping);
                return new S3ClientResources(client, createS3PreSigner(config, client, mapping));
            });
        }
        catch (ExecutionException e) {
            throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    private static void closeClientResources(S3ClientResources resources)
    {
        try {
            resources.close();
        }
        catch (RuntimeException e) {
            log.warn(e, "Failed to close S3 client resources");
        }
    }

    private record S3ClientResources(S3Client client, S3Presigner preSigner)
    {
        private S3ClientResources
        {
            requireNonNull(client, "client is null");
            requireNonNull(preSigner, "preSigner is null");
        }

        private void close()
        {
            RuntimeException failure = null;
            try {
                preSigner.close();
            }
            catch (RuntimeException e) {
                failure = e;
            }
            try {
                client.close();
            }
            catch (RuntimeException e) {
                if (failure != null) {
                    failure.addSuppressed(e);
                }
                else {
                    failure = e;
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    Cache<Optional<S3SecurityMappingResult>, S3ClientResources> clientResources()
    {
        return clientResources;
    }

    long maxClientResourcesCacheSize()
    {
        return MAX_CLIENT_RESOURCES_CACHE_SIZE;
    }

    S3Client createClient()
    {
        return clientFactory.create(Optional.empty());
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
        S3AuthType authType = config.getAuthType();
        Optional<String> staticIamRole = Optional.ofNullable(config.getIamRole());
        String staticRoleSessionName = config.getRoleSessionName();
        String externalId = config.getExternalId();
        Optional<S3AuthSchemeProvider> authSchemeProvider = config.getSignerType().map(S3FileSystemLoader::createAuthSchemeProvider);

        return mapping -> {
            Optional<AwsCredentialsProvider> credentialsProvider = mapping
                    .flatMap(S3SecurityMappingResult::credentialsProvider)
                    .or(() -> staticCredentialsProvider);

            Optional<String> region = mapping.flatMap(S3SecurityMappingResult::region).or(() -> staticRegion);
            Optional<String> endpoint = mapping.flatMap(S3SecurityMappingResult::endpoint).or(() -> staticEndpoint);

            Optional<String> iamRole = mapping.flatMap(S3SecurityMappingResult::iamRole).or(() -> staticIamRole);
            String roleSessionName = mapping.flatMap(S3SecurityMappingResult::roleSessionName).orElse(staticRoleSessionName);

            boolean crossRegionAccessEnabled = mapping
                    .flatMap(S3SecurityMappingResult::crossRegionAccessEnabled)
                    .orElse(config.isCrossRegionAccessEnabled());

            boolean pathStyleAccess = mapping
                    .flatMap(S3SecurityMappingResult::pathStyleAccess)
                    .orElse(config.isPathStyleAccess());

            S3ClientBuilder s3 = S3Client.builder();
            s3.overrideConfiguration(overrideConfiguration);
            s3.crossRegionAccessEnabled(crossRegionAccessEnabled);
            s3.httpClient(httpClient);
            s3.responseChecksumValidation(WHEN_REQUIRED);
            s3.requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED);
            s3.addPlugin(LegacyMd5Plugin.create());
            authSchemeProvider.ifPresent(s3::authSchemeProvider);

            region.map(Region::of).ifPresent(s3::region);
            endpoint.map(URI::create).ifPresent(s3::endpointOverride);
            s3.forcePathStyle(pathStyleAccess);

            switch (authType) {
                case ANONYMOUS -> s3.credentialsProvider(AnonymousCredentialsProvider.create());
                case WEB_IDENTITY -> s3.credentialsProvider(WebIdentityTokenFileCredentialsProvider.builder()
                        .asyncCredentialUpdateEnabled(true)
                        .build());
                case IAM_ROLE, DEFAULT -> {
                    // A security mapping may supply a per-request IAM role even when the static auth type is DEFAULT.
                    if (iamRole.isPresent()) {
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
                }
            }

            return s3.build();
        };
    }

    private static ClientOverrideConfiguration createOverrideConfiguration(OpenTelemetry openTelemetry, S3FileSystemConfig config, MetricPublisher metricPublisher)
    {
        return ClientOverrideConfiguration.builder()
                .addExecutionInterceptor(AwsSdkTelemetry.builder(openTelemetry)
                        .setCaptureExperimentalSpanAttributes(true)
                        .setRecordIndividualHttpError(true)
                        .build().createExecutionInterceptor())
                .retryStrategy(getRetryStrategy(config.getRetryMode()).toBuilder()
                        .maxAttempts(config.getMaxErrorRetries())
                        .build())
                .appId(config.getApplicationId())
                .addMetricPublisher(metricPublisher)
                .build();
    }

    static S3AuthSchemeProvider createAuthSchemeProvider(SignerType signerType)
    {
        Map<SignerProperty<?>, Object> signerProperties = switch (signerType) {
            case AwsS3V4Signer -> ImmutableMap.of();
            case Aws4Signer, AsyncAws4Signer, EventStreamAws4Signer -> ImmutableMap.of(
                    AwsV4HttpSigner.DOUBLE_URL_ENCODE, true,
                    AwsV4HttpSigner.NORMALIZE_PATH, true);
            case Aws4UnsignedPayloadSigner -> ImmutableMap.of(
                    AwsV4HttpSigner.DOUBLE_URL_ENCODE, true,
                    AwsV4HttpSigner.NORMALIZE_PATH, true,
                    AwsV4HttpSigner.PAYLOAD_SIGNING_ENABLED, false);
        };

        S3AuthSchemeProvider delegate = S3AuthSchemeProvider.defaultProvider();
        return params -> delegate.resolveAuthScheme(params).stream()
                .map(option -> applySignerProperties(option, signerProperties))
                .collect(toImmutableList());
    }

    private static AuthSchemeOption applySignerProperties(AuthSchemeOption option, Map<SignerProperty<?>, Object> signerProperties)
    {
        if (signerProperties.isEmpty() || !option.schemeId().equals(AwsV4AuthScheme.SCHEME_ID)) {
            return option;
        }
        AuthSchemeOption.Builder builder = option.toBuilder();
        signerProperties.forEach((property, value) -> putSignerProperty(builder, property, value));
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private static <T> void putSignerProperty(AuthSchemeOption.Builder builder, SignerProperty<?> property, Object value)
    {
        builder.putSignerProperty((SignerProperty<T>) property, (T) value);
    }

    private static SdkHttpClient createHttpClient(S3FileSystemConfig config)
    {
        ApacheHttpClient.Builder client = ApacheHttpClient.builder()
                .maxConnections(config.getMaxConnections())
                .tcpKeepAlive(config.getTcpKeepAlive());

        config.getConnectionTtl().ifPresent(ttl -> client.connectionTimeToLive(ttl.toJavaTime()));
        config.getConnectionMaxIdleTime().ifPresent(time -> client.connectionMaxIdleTime(time.toJavaTime()));
        config.getSocketConnectTimeout().ifPresent(timeout -> client.connectionTimeout(timeout.toJavaTime()));
        config.getSocketTimeout().ifPresent(timeout -> client.socketTimeout(timeout.toJavaTime()));

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
