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
package io.trino.plugin.elasticsearch.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.inject.Inject;

public class IamUserCredentialsProvider
        implements AwsSignerCredentialsProvider
{
    private final AwsSecurityConfig awsSecurityConfig;

    @Inject
    public IamUserCredentialsProvider(AwsSecurityConfig awsSecurityConfig)
    {
        this.awsSecurityConfig = awsSecurityConfig;
    }

    @Override
    public String getAwsRegion()
    {
        return awsSecurityConfig.getRegion();
    }

    @Override
    public AWSCredentialsProvider getCredentialsProvider()
    {
        if (awsSecurityConfig.getAccessKey().isPresent() && awsSecurityConfig.getSecretKey().isPresent()) {
            return new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                    awsSecurityConfig.getAccessKey().get(),
                    awsSecurityConfig.getSecretKey().get()));
        }
        return DefaultAWSCredentialsProviderChain.getInstance();
    }
}
