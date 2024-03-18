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
package io.varada.cloudstorage.s3;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.ConfigurationFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorContext;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.lang.annotation.Annotation;
import java.net.URI;
import java.util.Optional;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class S3CloudStorageModule
        implements Module
{
    private final ConnectorContext context;
    private final ConfigurationFactory configurationFactory;
    private final Class<? extends Annotation> annotation;

    public S3CloudStorageModule(ConnectorContext context,
                                ConfigurationFactory configurationFactory,
                                Class<? extends Annotation> annotation)
    {
        this.context = requireNonNull(context, "context is null");
        this.configurationFactory = requireNonNull(configurationFactory, "configurationFactory is null");
        this.annotation = requireNonNull(annotation, "annotation is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(ConfigurationFactory.class).toInstance(configurationFactory);

        configBinder(binder).bindConfig(S3FileSystemConfig.class);

        binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
        binder.bind(CatalogHandle.class).toInstance(context.getCatalogHandle());
        binder.bind(S3FileSystemFactory.class);

        binder.bind(S3CloudStorage.class).annotatedWith(annotation).to(S3CloudStorage.class);
    }

    @Provides
    @Singleton
    public S3CloudStorage provideS3CloudStorage(S3FileSystemFactory fileSystemFactory, S3FileSystemConfig config)
    {
        S3FileSystemFactory s3FileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        S3AsyncClient client = createS3AsyncClient(config);
        return new S3CloudStorage(s3FileSystemFactory, client);
    }

    private S3AsyncClient createS3AsyncClient(S3FileSystemConfig config)
    {
        S3CrtAsyncClientBuilder s3 = S3AsyncClient.crtBuilder();

        Optional<StaticCredentialsProvider> staticCredentialsProvider = getStaticCredentialsProvider(config);
        staticCredentialsProvider.ifPresent(s3::credentialsProvider);

        Optional.ofNullable(config.getRegion()).map(Region::of).ifPresent(s3::region);
        Optional.ofNullable(config.getEndpoint()).map(URI::create).ifPresent(s3::endpointOverride);
        s3.forcePathStyle(config.isPathStyleAccess());

        if (config.getIamRole() != null) {
            s3.credentialsProvider(getStsAssumeRoleCredentialsProvider(config));
        }

        return s3.build();
    }

    private static Optional<StaticCredentialsProvider> getStaticCredentialsProvider(S3FileSystemConfig config)
    {
        if ((config.getAwsAccessKey() != null) || (config.getAwsSecretKey() != null)) {
            return Optional.of(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.getAwsAccessKey(), config.getAwsSecretKey())));
        }
        return Optional.empty();
    }

    private static StsAssumeRoleCredentialsProvider getStsAssumeRoleCredentialsProvider(S3FileSystemConfig config)
    {
        StsClientBuilder sts = StsClient.builder();

        Optional.ofNullable(config.getStsEndpoint()).map(URI::create).ifPresent(sts::endpointOverride);
        Optional.ofNullable(config.getStsRegion())
                .or(() -> Optional.ofNullable(config.getRegion()))
                .map(Region::of).ifPresent(sts::region);

        Optional<StaticCredentialsProvider> staticCredentialsProvider = getStaticCredentialsProvider(config);
        staticCredentialsProvider.ifPresent(sts::credentialsProvider);

        return StsAssumeRoleCredentialsProvider.builder()
                .refreshRequest(request -> request
                        .roleArn(config.getIamRole())
                        .roleSessionName(config.getRoleSessionName())
                        .externalId(config.getExternalId()))
                .stsClient(sts.build())
                .asyncCredentialUpdateEnabled(true)
                .build();
    }
}
