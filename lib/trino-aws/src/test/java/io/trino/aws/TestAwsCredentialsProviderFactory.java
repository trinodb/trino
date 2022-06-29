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
package io.trino.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestAwsCredentialsProviderFactory
{
    @Test
    void customProvider()
    {
        AWSCredentialsProvider provider = new AwsCredentialsProviderFactory().createAwsCredentialsProvider(
                new AwsCredentialsProviderConfig()
                        .setCustomCredentialsProvider(CustomProvider.class.getName())
                        .setAccessKey("access key")
                        .setSecretKey("secret key")
                        .setSessionToken("session token")
                        .setIamRole("AIM role")
                        .setExternalId("external ID"));
        //noinspection unchecked
        Assertions.assertThat(provider)
                .isInstanceOf(CustomProvider.class)
                .extracting(AWSCredentialsProvider::getCredentials)
                .extracting(AWSCredentials::getAWSAccessKeyId, AWSCredentials::getAWSSecretKey)
                .containsExactly("default", "credentials");
    }

    @Test
    void customProviderWithCustomResolver()
    {
        AWSCredentialsProvider provider = new AwsCredentialsProviderFactory().createAwsCredentialsProvider(
                new AwsCredentialsProviderConfig()
                        .setCustomCredentialsProvider(CustomProvider.class.getName())
                        .setCustomProviderResolver(providerClass -> Class.forName(providerClass)
                                .asSubclass(CustomProvider.class)
                                .getConstructor(AWSCredentials.class)
                                .newInstance(new BasicAWSCredentials("custom", "credentials")))
                        .setAccessKey("access key")
                        .setSecretKey("secret key")
                        .setSessionToken("session token")
                        .setIamRole("IAM role")
                        .setExternalId("external ID"));

        //noinspection unchecked
        assertThat(provider)
                .isInstanceOf(CustomProvider.class)
                .extracting(AWSCredentialsProvider::getCredentials)
                .extracting(AWSCredentials::getAWSAccessKeyId, AWSCredentials::getAWSSecretKey)
                .containsExactly("custom", "credentials");
    }

    @Test
    void embeddedCredentials()
    {
        AWSCredentialsProvider provider = new AwsCredentialsProviderFactory().createAwsCredentialsProvider(
                new AwsCredentialsProviderConfig()
                        .setServiceUri(URI.create("proto://embedded:credentials@service/"))
                        .setCustomCredentialsProvider(CustomProvider.class.getName()));
        //noinspection unchecked
        Assertions.assertThat(provider)
                .isInstanceOf(AWSStaticCredentialsProvider.class)
                .extracting(AWSCredentialsProvider::getCredentials)
                .extracting(AWSCredentials::getAWSAccessKeyId, AWSCredentials::getAWSSecretKey)
                .containsExactly("embedded", "credentials");
    }

    @Test
    void embeddedCredentialsInvalid()
    {
        AWSCredentialsProvider provider = new AwsCredentialsProviderFactory().createAwsCredentialsProvider(
                new AwsCredentialsProviderConfig()
                        .setServiceUri(URI.create("proto://service"))
                        .setCustomCredentialsProvider(CustomProvider.class.getName()));
        assertThat(provider)
                .isInstanceOf(CustomProvider.class);
    }

    @Test
    void fixedCredentials()
    {
        AWSCredentialsProvider provider = new AwsCredentialsProviderFactory().createAwsCredentialsProvider(
                new AwsCredentialsProviderConfig()
                        .setAccessKey("access key")
                        .setSecretKey("secret key"));
        //noinspection unchecked
        Assertions.assertThat(provider)
                .isInstanceOf(AWSStaticCredentialsProvider.class)
                .extracting(AWSCredentialsProvider::getCredentials)
                .extracting(AWSCredentials::getAWSAccessKeyId, AWSCredentials::getAWSSecretKey)
                .containsExactly("access key", "secret key");
    }

    @Test
    void fixedCredentialsWithSession()
    {
        AWSCredentialsProvider provider = new AwsCredentialsProviderFactory().createAwsCredentialsProvider(
                new AwsCredentialsProviderConfig()
                        .setAccessKey("access key")
                        .setSecretKey("secret key")
                        .setSessionToken("session token"));
        //noinspection unchecked
        Assertions.assertThat(provider)
                .isInstanceOf(AWSStaticCredentialsProvider.class)
                .extracting(AWSCredentialsProvider::getCredentials)
                .isInstanceOf(AWSSessionCredentials.class)
                .extracting(AWSSessionCredentials.class::cast)
                .extracting(AWSCredentials::getAWSAccessKeyId, AWSCredentials::getAWSSecretKey, AWSSessionCredentials::getSessionToken)
                .containsExactly("access key", "secret key", "session token");
    }

    @Test
    void fixedCredentialsMissingSecret()
    {
        assertThatThrownBy(() -> new AwsCredentialsProviderFactory().createAwsCredentialsProvider(
                new AwsCredentialsProviderConfig()
                        .setAccessKey("access key")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("AWS Secret Key");
    }

    @Test
    void fixedCredentialsMissingAccess()
    {
        assertThatThrownBy(() -> new AwsCredentialsProviderFactory().createAwsCredentialsProvider(
                new AwsCredentialsProviderConfig()
                        .setSecretKey("secret key")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("AWS Access Key");
    }

    @Test
    void fixedCredentialsWithRole()
    {
        AWSCredentialsProvider provider = new AwsCredentialsProviderFactory().createAwsCredentialsProvider(
                new AwsCredentialsProviderConfig()
                        .setAccessKey("access key")
                        .setSecretKey("secret key")
                        .setIamRole("AIM role")
                        .setRegion("region")
                        .setExternalId("external ID"));
        Assertions.assertThat(provider)
                // that's all we can assert here without using reflection
                .isInstanceOf(STSAssumeRoleSessionCredentialsProvider.class);
    }

    @Test
    void fixedCredentialsWithSessionAndRole()
    {
        AWSCredentialsProvider provider = new AwsCredentialsProviderFactory().createAwsCredentialsProvider(
                new AwsCredentialsProviderConfig()
                        .setAccessKey("access key")
                        .setSecretKey("secret key")
                        .setSessionToken("session token")
                        .setIamRole("AIM role")
                        .setRegion("region")
                        .setExternalId("external ID"));
        Assertions.assertThat(provider)
                // that's all we can assert here without using reflection
                .isInstanceOf(STSAssumeRoleSessionCredentialsProvider.class);
    }

    @Test
    void defaultCredentialsChain()
    {
        AWSCredentialsProvider provider = new AwsCredentialsProviderFactory().createAwsCredentialsProvider(
                new AwsCredentialsProviderConfig());
        Assertions.assertThat(provider)
                .isInstanceOf(DefaultAWSCredentialsProviderChain.class);
    }

    @Test
    void defaultCredentialsChainWithRole()
    {
        AWSCredentialsProvider provider = new AwsCredentialsProviderFactory().createAwsCredentialsProvider(
                new AwsCredentialsProviderConfig()
                        .setIamRole("AIM role")
                        .setRegion("region")
                        .setExternalId("external ID"));
        Assertions.assertThat(provider)
                // that's all we can assert here without using reflection
                .isInstanceOf(STSAssumeRoleSessionCredentialsProvider.class);
    }

    public static class CustomProvider
            implements AWSCredentialsProvider
    {
        private final AWSCredentials credentials;

        @SuppressWarnings("unused") // used reflectively
        public CustomProvider()
        {
            this(new BasicAWSCredentials("default", "credentials"));
        }

        public CustomProvider(AWSCredentials credentials)
        {
            this.credentials = credentials;
        }

        @Override
        public AWSCredentials getCredentials()
        {
            return credentials;
        }

        @Override
        public void refresh()
        {
        }
    }
}
