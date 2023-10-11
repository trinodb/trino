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
import io.trino.plugin.elasticsearch.PasswordConfig;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

import java.util.Optional;

public class AuthRestClientConfigurator
        implements ElasticRestClientConfigurator
{
    private final Optional<AwsSecurityConfig> awsSecurityConfig;
    private final Optional<PasswordConfig> passwordConfig;

    @Inject
    AuthRestClientConfigurator(Optional<AwsSecurityConfig> awsSecurityConfig, Optional<PasswordConfig> passwordConfig)
    {

        this.awsSecurityConfig = awsSecurityConfig;
        this.passwordConfig = passwordConfig;
    }

    @Override
    public void configure(HttpAsyncClientBuilder clientBuilder)
    {

        passwordConfig.ifPresent(securityConfig -> {
            CredentialsProvider credentials = new BasicCredentialsProvider();
            credentials.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(securityConfig.getUser(), securityConfig.getPassword()));
            clientBuilder.setDefaultCredentialsProvider(credentials);
        });

        awsSecurityConfig.ifPresent(securityConfig -> clientBuilder.addInterceptorLast(new AwsRequestSigner(
                securityConfig.getRegion(),
                getAwsCredentialsProvider(securityConfig))));
    }

    private AWSCredentialsProvider getAwsCredentialsProvider(AwsSecurityConfig config)
    {
        AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();

        if (config.getAccessKey().isPresent() && config.getSecretKey().isPresent()) {
            credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                    config.getAccessKey().get(),
                    config.getSecretKey().get()));
        }

        if (config.getIamRole().isPresent()) {
            STSAssumeRoleSessionCredentialsProvider.Builder credentialsProviderBuilder = new STSAssumeRoleSessionCredentialsProvider.Builder(config.getIamRole().get(), "trino-session")
                    .withStsClient(AWSSecurityTokenServiceClientBuilder.standard()
                            .withRegion(config.getRegion())
                            .withCredentials(credentialsProvider)
                            .build());
            config.getExternalId().ifPresent(credentialsProviderBuilder::withExternalId);
            credentialsProvider = credentialsProviderBuilder.build();
        }

        return credentialsProvider;
    }
}
