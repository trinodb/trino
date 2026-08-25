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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Map;

import static io.trino.plugin.iceberg.StsAwsCredentialProvider.AWS_IAM_ROLE;
import static io.trino.plugin.iceberg.StsAwsCredentialProvider.AWS_IAM_ROLE_SESSION_NAME;
import static io.trino.plugin.iceberg.StsAwsCredentialProvider.AWS_STS_ACCESS_KEY_ID;
import static io.trino.plugin.iceberg.StsAwsCredentialProvider.AWS_STS_ENDPOINT;
import static io.trino.plugin.iceberg.StsAwsCredentialProvider.AWS_STS_REGION;
import static io.trino.plugin.iceberg.StsAwsCredentialProvider.AWS_STS_SECRET_ACCESS_KEY;
import static io.trino.plugin.iceberg.StsAwsCredentialProvider.AWS_STS_SIGNER_REGION;
import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.testing.InterfaceTestUtils.assertProperForwardingMethodsAreCalled;
import static org.assertj.core.api.Assertions.assertThat;

class TestStsAwsCredentialProvider
{
    @AfterEach
    void tearDown()
    {
        StsAwsCredentialProvider.resetCache();
    }

    @Test
    void testEverythingImplemented()
    {
        assertAllMethodsOverridden(AwsCredentialsProvider.class, StsAwsCredentialProvider.class);
    }

    @Test
    void testProperForwardingMethodsAreCalled()
    {
        assertProperForwardingMethodsAreCalled(AwsCredentialsProvider.class, StsAwsCredentialProvider::new);
    }

    @Test
    void testCreateReusesStsClientForSameCatalogProperties()
    {
        Map<String, String> properties = catalogProperties("arn:aws:iam::000000000000:role/trino-s3tables");

        StsAwsCredentialProvider first = StsAwsCredentialProvider.create(properties);
        StsAwsCredentialProvider second = StsAwsCredentialProvider.create(properties);

        assertThat(first).isSameAs(second);
        assertThat(first.stsClient()).isSameAs(second.stsClient());
        assertThat(first.stsClient()).isNotNull();
    }

    @Test
    void testCreateUsesDistinctStsClientForDifferentRoles()
    {
        StsAwsCredentialProvider first = StsAwsCredentialProvider.create(
                catalogProperties("arn:aws:iam::000000000000:role/a"));
        StsAwsCredentialProvider second = StsAwsCredentialProvider.create(
                catalogProperties("arn:aws:iam::000000000000:role/b"));

        assertThat(first).isNotSameAs(second);
        assertThat(first.stsClient()).isNotSameAs(second.stsClient());
    }

    private static Map<String, String> catalogProperties(String roleArn)
    {
        return ImmutableMap.<String, String>builder()
                .put(AWS_IAM_ROLE, roleArn)
                .put(AWS_IAM_ROLE_SESSION_NAME, "trino-iceberg-rest-catalog")
                .put(AWS_STS_ENDPOINT, "http://127.0.0.1:4566")
                .put(AWS_STS_REGION, "us-east-1")
                .put(AWS_STS_SIGNER_REGION, "us-east-1")
                .put(AWS_STS_ACCESS_KEY_ID, "test")
                .put(AWS_STS_SECRET_ACCESS_KEY, "test")
                .buildOrThrow();
    }
}
