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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.iam.aws.IAMSecurityMapping;
import io.trino.iam.aws.IAMSecurityMappingProvider;
import io.trino.iam.aws.IAMSecurityMappingResult;
import io.trino.iam.aws.IAMSecurityMappings;
import io.trino.spi.security.ConnectorIdentity;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.auth.StsWebIdentityTokenFileCredentialsProvider;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class GlueClientFactory
{
    private final GlueHiveMetastoreConfig config;
    private final Optional<IAMSecurityMappingProvider<IAMSecurityMappings<IAMSecurityMapping>, IAMSecurityMapping, GlueSecurityMappingConfig>> mappingProvider;
    private final Optional<StaticCredentialsProvider> staticCredentialsProvider;
    private final @ForGlueHiveMetastore Set<ExecutionInterceptor> executionInterceptors;

    @Inject
    public GlueClientFactory(
            GlueHiveMetastoreConfig config,
            Optional<IAMSecurityMappingProvider<IAMSecurityMappings<IAMSecurityMapping>, IAMSecurityMapping, GlueSecurityMappingConfig>> mappingProvider,
            @ForGlueHiveMetastore Set<ExecutionInterceptor> executionInterceptors)
    {
        this.config = requireNonNull(config, "config is null");
        this.mappingProvider = mappingProvider;
        this.staticCredentialsProvider = getStaticCredentialsProvider(config);
        this.executionInterceptors = Set.copyOf(executionInterceptors);
    }

    public GlueClient create(ConnectorIdentity identity)
    {
        GlueClientBuilder builder = GlueClient.builder();

        builder.overrideConfiguration(override -> override
                .executionInterceptors(ImmutableList.copyOf(executionInterceptors))
                .retryStrategy(retryBuilder -> retryBuilder
                        .retryOnException(t -> t instanceof ConcurrentModificationException)
                        .maxAttempts(config.getMaxGlueErrorRetries())));

        Optional<AwsCredentialsProvider> credentials = resolveCredentials(identity);
        if (credentials.isPresent()) {
            builder.credentialsProvider(credentials.get());
        }
        else {
            staticCredentialsProvider.ifPresent(builder::credentialsProvider);
        }

        if (config.getGlueEndpointUrl().isPresent()) {
            builder.endpointOverride(config.getGlueEndpointUrl().get());
            builder.region(Region.of(config.getGlueRegion().orElseThrow()));
        }
        else if (config.getGlueRegion().isPresent()) {
            builder.region(Region.of(config.getGlueRegion().get()));
        }
        else if (config.getPinGlueClientToCurrentRegion()) {
            builder.region(DefaultAwsRegionProviderChain.builder().build().getRegion());
        }

        builder.httpClientBuilder(ApacheHttpClient.builder()
                .maxConnections(config.getMaxGlueConnections()));

        return builder.build();
    }

    private Optional<AwsCredentialsProvider> resolveCredentials(ConnectorIdentity identity)
    {
        if (mappingProvider.isPresent()) {
            Optional<IAMSecurityMappingResult> mapping = mappingProvider.flatMap(provider -> provider.getMapping(identity));
            if (mapping.isPresent()) {
                IAMSecurityMappingResult iamMapping = mapping.get();
                return Optional.of(StsAssumeRoleCredentialsProvider.builder()
                        .refreshRequest(req -> req
                                .roleArn(iamMapping.iamRole().orElseThrow())
                                .roleSessionName(iamMapping.roleSessionName().orElse("trino-session")))
                        .stsClient(getStsClient())
                        .asyncCredentialUpdateEnabled(true)
                        .build());
            }
        }

        if (config.isUseWebIdentityTokenCredentialsProvider()) {
            return Optional.of(StsWebIdentityTokenFileCredentialsProvider.builder()
                    .stsClient(getStsClient())
                    .asyncCredentialUpdateEnabled(true)
                    .build());
        }

        if (config.getIamRole().isPresent()) {
            return Optional.of(StsAssumeRoleCredentialsProvider.builder()
                    .refreshRequest(req -> req
                            .roleArn(config.getIamRole().get())
                            .roleSessionName("trino-session")
                            .externalId(config.getExternalId().orElse(null)))
                    .stsClient(getStsClient())
                    .asyncCredentialUpdateEnabled(true)
                    .build());
        }

        return Optional.empty();
    }

    private StsClient getStsClient()
    {
        StsClientBuilder sts = StsClient.builder();
        staticCredentialsProvider.ifPresent(sts::credentialsProvider);

        if (config.getGlueStsEndpointUrl().isPresent() && config.getGlueStsRegion().isPresent()) {
            return sts.endpointOverride(config.getGlueStsEndpointUrl().get())
                    .region(Region.of(config.getGlueStsRegion().get()))
                    .build();
        }

        if (config.getGlueStsRegion().isPresent()) {
            return sts.region(Region.of(config.getGlueStsRegion().get())).build();
        }

        if (config.getPinGlueClientToCurrentRegion()) {
            return sts.region(DefaultAwsRegionProviderChain.builder().build().getRegion()).build();
        }

        return sts.build();
    }

    private static Optional<StaticCredentialsProvider> getStaticCredentialsProvider(GlueHiveMetastoreConfig config)
    {
        if (config.getAwsAccessKey().isPresent() && config.getAwsSecretKey().isPresent()) {
            return Optional.of(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.getAwsAccessKey().get(), config.getAwsSecretKey().get())));
        }
        return Optional.empty();
    }
}
