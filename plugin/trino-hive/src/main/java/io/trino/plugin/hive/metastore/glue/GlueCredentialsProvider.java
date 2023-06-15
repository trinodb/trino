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

import com.google.inject.Inject;
import com.google.inject.Provider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.net.URI;

import static java.lang.String.format;

public class GlueCredentialsProvider
        implements Provider<AwsCredentialsProvider>
{
    private final AwsCredentialsProvider credentialsProvider;

    @Inject
    public GlueCredentialsProvider(GlueHiveMetastoreConfig config)
    {
        if (config.getAwsCredentialsProvider().isPresent()) {
            this.credentialsProvider = getCustomAWSCredentialsProvider(config.getAwsCredentialsProvider().get());
        }
        else {
            AwsCredentialsProvider provider;
            if (config.getAwsAccessKey().isPresent() && config.getAwsSecretKey().isPresent()) {
                provider = StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(config.getAwsAccessKey().get(), config.getAwsSecretKey().get()));
            }
            else {
                provider = DefaultCredentialsProvider.create();
            }
            if (config.getIamRole().isPresent()) {
                StsClientBuilder stsClientBuilder = StsClient.builder()
                        .credentialsProvider(provider);

                if (config.getGlueStsEndpointUrl().isPresent() && config.getGlueStsRegion().isPresent()) {
                    stsClientBuilder.endpointOverride(URI.create(config.getGlueStsEndpointUrl().get()))
                            .region(Region.of(config.getGlueStsRegion().get()));
                }
                else if (config.getGlueStsRegion().isPresent()) {
                    stsClientBuilder.region(Region.of(config.getGlueStsRegion().get()));
                }

                provider = StsAssumeRoleCredentialsProvider.builder()
                        .refreshRequest(() -> AssumeRoleRequest
                                .builder()
                                .roleArn(config.getIamRole().get())
                                .roleSessionName("trino-session")
                                .externalId(config.getExternalId().orElse(null))
                                .build())
                        .stsClient(stsClientBuilder.build())
                        .build();
            }
            this.credentialsProvider = provider;
        }
    }

    @Override
    public AwsCredentialsProvider get()
    {
        return credentialsProvider;
    }

    private static AwsCredentialsProvider getCustomAWSCredentialsProvider(String providerClass)
    {
        try {
            Object instance = Class.forName(providerClass).getConstructor().newInstance();
            if (!(instance instanceof AwsCredentialsProvider)) {
                throw new RuntimeException("Invalid credentials provider class: " + instance.getClass().getName());
            }
            return (AwsCredentialsProvider) instance;
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(format("Error creating an instance of %s", providerClass), e);
        }
    }
}
