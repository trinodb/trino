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
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.net.URI;
import java.util.Optional;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static java.lang.Math.toIntExact;

public final class S3FileSystemFactory
        implements TrinoFileSystemFactory
{
    private final S3Client client;
    private final S3Context context;

    @Inject
    public S3FileSystemFactory(OpenTelemetry openTelemetry, S3FileSystemConfig config)
    {
        S3ClientBuilder s3 = S3Client.builder();

        s3.overrideConfiguration(ClientOverrideConfiguration.builder()
                .addExecutionInterceptor(AwsSdkTelemetry.builder(openTelemetry)
                        .setCaptureExperimentalSpanAttributes(true)
                        .setRecordIndividualHttpError(true)
                        .build().newExecutionInterceptor())
                .build());

        Optional<StaticCredentialsProvider> staticCredentialsProvider = getStaticCredentialsProvider(config);
        staticCredentialsProvider.ifPresent(s3::credentialsProvider);

        Optional.ofNullable(config.getRegion()).map(Region::of).ifPresent(s3::region);
        Optional.ofNullable(config.getEndpoint()).map(URI::create).ifPresent(s3::endpointOverride);
        s3.forcePathStyle(config.isPathStyleAccess());

        if (config.getIamRole() != null) {
            StsClientBuilder sts = StsClient.builder();
            Optional.ofNullable(config.getStsEndpoint()).map(URI::create).ifPresent(sts::endpointOverride);
            Optional.ofNullable(config.getStsRegion())
                    .or(() -> Optional.ofNullable(config.getRegion()))
                    .map(Region::of).ifPresent(sts::region);
            staticCredentialsProvider.ifPresent(sts::credentialsProvider);

            s3.credentialsProvider(StsAssumeRoleCredentialsProvider.builder()
                    .refreshRequest(request -> request
                            .roleArn(config.getIamRole())
                            .roleSessionName(config.getRoleSessionName())
                            .externalId(config.getExternalId()))
                    .stsClient(sts.build())
                    .asyncCredentialUpdateEnabled(true)
                    .build());
        }

        ApacheHttpClient.Builder httpClient = ApacheHttpClient.builder()
                .maxConnections(config.getMaxConnections())
                .tcpKeepAlive(config.getTcpKeepAlive());

        config.getConnectionTtl().ifPresent(connectionTtl -> httpClient.connectionTimeToLive(connectionTtl.toJavaTime()));
        config.getConnectionMaxIdleTime().ifPresent(connectionMaxIdleTime -> httpClient.connectionMaxIdleTime(connectionMaxIdleTime.toJavaTime()));
        config.getSocketConnectTimeout().ifPresent(socketConnectTimeout -> httpClient.connectionTimeout(socketConnectTimeout.toJavaTime()));
        config.getSocketReadTimeout().ifPresent(socketReadTimeout -> httpClient.socketTimeout(socketReadTimeout.toJavaTime()));

        if (config.getHttpProxy() != null) {
            URI endpoint = URI.create("%s://%s".formatted(
                    config.isHttpProxySecure() ? "https" : "http",
                    config.getHttpProxy()));
            httpClient.proxyConfiguration(ProxyConfiguration.builder()
                    .endpoint(endpoint)
                    .build());
        }

        s3.httpClientBuilder(httpClient);

        this.client = s3.build();

        context = new S3Context(
                toIntExact(config.getStreamingPartSize().toBytes()),
                config.isRequesterPays(),
                config.getSseType(),
                config.getSseKmsKeyId(),
                Optional.empty());
    }

    @PreDestroy
    public void destroy()
    {
        client.close();
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        if (identity.getExtraCredentials().containsKey(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY)) {
            AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsSessionCredentials.create(
                    identity.getExtraCredentials().get(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY),
                    identity.getExtraCredentials().get(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY),
                    identity.getExtraCredentials().get(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY)));
            return new S3FileSystem(client, context.withCredentialsProviderOverride(credentialsProvider));
        }

        return new S3FileSystem(client, context);
    }

    private static Optional<StaticCredentialsProvider> getStaticCredentialsProvider(S3FileSystemConfig config)
    {
        if ((config.getAwsAccessKey() != null) || (config.getAwsSecretKey() != null)) {
            return Optional.of(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.getAwsAccessKey(), config.getAwsSecretKey())));
        }
        return Optional.empty();
    }
}
