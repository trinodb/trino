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
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.net.URI;
import java.util.Optional;

import static java.lang.Math.toIntExact;

public final class S3FileSystemFactory
        implements TrinoFileSystemFactory
{
    private final S3Client client;
    private final S3AsyncClient asyncClient;
    private final S3Context context;

    @Inject
    public S3FileSystemFactory(S3FileSystemConfig config)
    {
        S3ClientBuilder s3 = S3Client.builder();
        applyS3Properties(s3, config);
        s3.httpClient(buildHttpClient(config));

        S3AsyncClientBuilder asyncS3 = S3AsyncClient.builder();
        applyS3Properties(asyncS3, config);
        asyncS3.httpClient(buildAsyncHttpClient(config));

        this.client = s3.build();
        this.asyncClient = asyncS3.build();

        context = new S3Context(
                toIntExact(config.getStreamingPartSize().toBytes()),
                config.isRequesterPays(),
                config.getSseType(),
                config.getSseKmsKeyId());

    }

    private static void applyS3Properties(S3BaseClientBuilder<?, ?> s3, S3FileSystemConfig config)
    {
        if ((config.getAwsAccessKey() != null) && (config.getAwsSecretKey() != null)) {
            s3.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.getAwsAccessKey(), config.getAwsSecretKey())));
        }

        Optional.ofNullable(config.getRegion()).map(Region::of).ifPresent(s3::region);
        Optional.ofNullable(config.getEndpoint()).map(URI::create).ifPresent(s3::endpointOverride);
        s3.forcePathStyle(config.isPathStyleAccess());

        if (config.getIamRole() != null) {
            StsClientBuilder sts = StsClient.builder();
            Optional.ofNullable(config.getStsEndpoint()).map(URI::create).ifPresent(sts::endpointOverride);
            Optional.ofNullable(config.getStsRegion())
                    .or(() -> Optional.ofNullable(config.getRegion()))
                    .map(Region::of).ifPresent(sts::region);

            s3.credentialsProvider(StsAssumeRoleCredentialsProvider.builder()
                    .refreshRequest(request -> request
                            .roleArn(config.getIamRole())
                            .roleSessionName(config.getRoleSessionName())
                            .externalId(config.getExternalId()))
                    .stsClient(sts.build())
                    .asyncCredentialUpdateEnabled(true)
                    .build());
        }
    }

    private static SdkHttpClient buildHttpClient(S3FileSystemConfig config)
    {
        ApacheHttpClient.Builder httpClient = ApacheHttpClient.builder()
                .maxConnections(config.getMaxConnections());

        if (config.getHttpProxy() != null) {
            URI endpoint = URI.create("%s://%s".formatted(
                    config.isHttpProxySecure() ? "https" : "http",
                    config.getHttpProxy()));
            httpClient.proxyConfiguration(ProxyConfiguration.builder()
                    .endpoint(endpoint)
                    .build());
        }

        return httpClient.build();
    }

    private static SdkAsyncHttpClient buildAsyncHttpClient(S3FileSystemConfig config)
    {
        AwsCrtAsyncHttpClient.Builder httpClient = AwsCrtAsyncHttpClient.builder();
        if (config.getHttpProxy() != null) {
            String scheme = config.isHttpProxySecure() ? "https" : "http";
            httpClient.proxyConfiguration(software.amazon.awssdk.http.crt.ProxyConfiguration.builder()
                    .scheme(scheme)
                    .host(config.getHttpProxy().getHost())
                    .port(config.getHttpProxy().getPort())
                    .build());
        }

        return httpClient.build();
    }

    @PreDestroy
    public void destroy()
    {
        client.close();
        asyncClient.close();
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new S3FileSystem(client, asyncClient, context);
    }
}
