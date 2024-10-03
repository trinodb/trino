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
package io.trino.plugin.elasticsearch.client;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.google.inject.Inject;
import io.trino.plugin.elasticsearch.AwsSecurityConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

import java.util.Optional;

public class AwsSecurityRestClientConfigurator
        implements ElasticRestClientConfigurator
{
    private final String region;
    private final Optional<String> accessKey;
    private final Optional<String> secretKey;
    private final Optional<String> iamRole;
    private final Optional<String> externalId;

    @Inject
    AwsSecurityRestClientConfigurator(AwsSecurityConfig awsSecurityConfig)
    {
        this.region = awsSecurityConfig.getRegion();
        this.accessKey = awsSecurityConfig.getAccessKey();
        this.secretKey = awsSecurityConfig.getSecretKey();
        this.iamRole = awsSecurityConfig.getIamRole();
        this.externalId = awsSecurityConfig.getExternalId();
    }

    @Override
    public void configure(HttpAsyncClientBuilder clientBuilder)
    {
        clientBuilder.addInterceptorLast(new AwsRequestSigner(region, getAwsCredentialsProvider()));
    }

    private AWSCredentialsProvider getAwsCredentialsProvider()
    {
        AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();

        if (accessKey.isPresent() && secretKey.isPresent()) {
            credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                    accessKey.get(),
                    secretKey.get()));
        }

        if (iamRole.isPresent()) {
            STSAssumeRoleSessionCredentialsProvider.Builder credentialsProviderBuilder = new STSAssumeRoleSessionCredentialsProvider.Builder(iamRole.get(), "trino-session")
                    .withStsClient(AWSSecurityTokenServiceClientBuilder.standard()
                            .withRegion(region)
                            .withCredentials(credentialsProvider)
                            .build());
            externalId.ifPresent(credentialsProviderBuilder::withExternalId);
            credentialsProvider = credentialsProviderBuilder.build();
        }

        return credentialsProvider;
    }
}
