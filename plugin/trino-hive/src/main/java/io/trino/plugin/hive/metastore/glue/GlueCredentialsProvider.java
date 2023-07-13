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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.google.inject.Inject;
import com.google.inject.Provider;

import static io.trino.hdfs.s3.AwsCurrentRegionHolder.getCurrentRegionFromEC2Metadata;
import static java.lang.String.format;

public class GlueCredentialsProvider
        implements Provider<AWSCredentialsProvider>
{
    private final AWSCredentialsProvider credentialsProvider;

    @Inject
    public GlueCredentialsProvider(GlueHiveMetastoreConfig config)
    {
        if (config.getAwsCredentialsProvider().isPresent()) {
            this.credentialsProvider = getCustomAWSCredentialsProvider(config.getAwsCredentialsProvider().get());
        }
        else {
            AWSCredentialsProvider provider;
            if (config.getAwsAccessKey().isPresent() && config.getAwsSecretKey().isPresent()) {
                provider = new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(config.getAwsAccessKey().get(), config.getAwsSecretKey().get()));
            }
            else {
                provider = DefaultAWSCredentialsProviderChain.getInstance();
            }
            if (config.getIamRole().isPresent()) {
                AWSSecurityTokenServiceClientBuilder stsClientBuilder = AWSSecurityTokenServiceClientBuilder
                        .standard()
                        .withCredentials(provider);

                if (config.getGlueStsEndpointUrl().isPresent() && config.getGlueStsRegion().isPresent()) {
                    stsClientBuilder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(config.getGlueStsEndpointUrl().get(), config.getGlueStsRegion().get()));
                }
                else if (config.getGlueStsRegion().isPresent()) {
                    stsClientBuilder.setRegion(config.getGlueStsRegion().get());
                }
                else if (config.getPinGlueClientToCurrentRegion()) {
                    stsClientBuilder.setRegion(getCurrentRegionFromEC2Metadata().getName());
                }

                provider = new STSAssumeRoleSessionCredentialsProvider
                        .Builder(config.getIamRole().get(), "trino-session")
                        .withExternalId(config.getExternalId().orElse(null))
                        .withStsClient(stsClientBuilder.build())
                        .build();
            }
            this.credentialsProvider = provider;
        }
    }

    @Override
    public AWSCredentialsProvider get()
    {
        return credentialsProvider;
    }

    private static AWSCredentialsProvider getCustomAWSCredentialsProvider(String providerClass)
    {
        try {
            Object instance = Class.forName(providerClass).getConstructor().newInstance();
            if (!(instance instanceof AWSCredentialsProvider)) {
                throw new RuntimeException("Invalid credentials provider class: " + instance.getClass().getName());
            }
            return (AWSCredentialsProvider) instance;
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(format("Error creating an instance of %s", providerClass), e);
        }
    }
}
