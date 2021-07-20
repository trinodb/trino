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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestIamUserCredentialsProvider
{
    @Test
    public void testGetAwsRegion()
    {
        String awsRegion = "us-east-2";
        AwsSecurityConfig awsConfig = new AwsSecurityConfig().setRegion(awsRegion);
        IamUserCredentialsProvider instance = new IamUserCredentialsProvider(awsConfig);

        assertThat(instance.getAwsRegion()).isEqualTo(awsRegion);
    }

    @Test
    public void testGetCredentialsProviderWithConfiguredCredentials()
    {
        AwsSecurityConfig awsConfig = new AwsSecurityConfig()
                .setAccessKey("access")
                .setSecretKey("secret");
        IamUserCredentialsProvider instance = new IamUserCredentialsProvider(awsConfig);

        assertThat(instance.getCredentialsProvider()).isInstanceOf(AWSStaticCredentialsProvider.class);
        assertThat(instance.getCredentialsProvider().getCredentials().getAWSAccessKeyId()).isEqualTo("access");
        assertThat(instance.getCredentialsProvider().getCredentials().getAWSSecretKey()).isEqualTo("secret");
    }

    @Test
    public void testGetCredentialsProviderWithNoConfiguredCredentials()
    {
        AwsSecurityConfig awsConfig = new AwsSecurityConfig();
        IamUserCredentialsProvider instance = new IamUserCredentialsProvider(awsConfig);

        assertThat(instance.getCredentialsProvider()).isInstanceOf(DefaultAWSCredentialsProviderChain.class);
    }
}
