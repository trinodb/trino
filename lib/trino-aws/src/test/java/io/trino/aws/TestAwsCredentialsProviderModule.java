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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.google.inject.BindingAnnotation;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.airlift.bootstrap.Bootstrap;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Map;
import java.util.Optional;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.assertj.core.api.Assertions.assertThat;

class TestAwsCredentialsProviderModule
{
    @Test
    void customUnqualifiedBindings()
    {
        Injector injector = new Bootstrap(new AwsCredentialsProviderFactoryModule(), new AwsCredentialsProviderModule("test"))
                .setRequiredConfigurationProperties(Map.of(
                        "test.access-key", "access key",
                        "test.secret-key", "secret key",
                        "test.session-token", "session token",
                        "test.iam-role", "AIM role",
                        "test.external-id", "external ID",
                        "test.region", "us-east-2"))
                .initialize();

        AWSCredentialsProvider provider = injector.getInstance(AWSCredentialsProvider.class);
        AwsCredentialsProviderConfig config = injector.getInstance(AwsCredentialsProviderConfig.class);

        assertThat(provider)
                .isInstanceOf(STSAssumeRoleSessionCredentialsProvider.class);

        assertThat(config)
                .extracting(AwsCredentialsProviderConfig::getAccessKey)
                .isEqualTo(Optional.of("access key"));
    }

    @Test
    void customQualifiedBindings()
    {
        Injector injector = new Bootstrap(new AwsCredentialsProviderFactoryModule(), new AwsCredentialsProviderModule("test", ForTest.class))
                .setRequiredConfigurationProperties(Map.of(
                        "test.access-key", "access key",
                        "test.secret-key", "secret key",
                        "test.session-token", "session token",
                        "test.iam-role", "AIM role",
                        "test.external-id", "external ID",
                        "test.region", "us-east-2"))
                .initialize();

        AWSCredentialsProvider provider = injector.getInstance(Key.get(AWSCredentialsProvider.class, ForTest.class));
        AwsCredentialsProviderConfig config = injector.getInstance(Key.get(AwsCredentialsProviderConfig.class, ForTest.class));

        assertThat(provider)
                .isInstanceOf(STSAssumeRoleSessionCredentialsProvider.class);

        assertThat(config)
                .extracting(AwsCredentialsProviderConfig::getAccessKey)
                .isEqualTo(Optional.of("access key"));
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    @interface ForTest
    {
    }
}
