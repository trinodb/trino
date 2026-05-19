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
package io.trino.plugin.iceberg.catalog.rest;

import org.junit.jupiter.api.Test;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergRestCatalogSigV4Config
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(IcebergRestCatalogSigV4Config.class)
                .setSigningName("execute-api")
                .setStsWebIdentity(false)
                .setStsRoleArn(null)
                .setStsPolicy(null)
                .setStsDurationSeconds(null));
    }

    @Test
    void testPropertyMappings()
    {
        IcebergRestCatalogSigV4Config config = new IcebergRestCatalogSigV4Config()
                .setSigningName("glue")
                .setStsWebIdentity(true)
                .setStsRoleArn("arn:aws:iam::123456789012:role/my-role")
                .setStsDurationSeconds(3600);

        assertThat(config.getSigningName()).isEqualTo("glue");
        assertThat(config.isStsWebIdentity()).isTrue();
        assertThat(config.getStsRoleArn()).hasValue("arn:aws:iam::123456789012:role/my-role");
        assertThat(config.getStsPolicy()).isEmpty();
        assertThat(config.getStsDurationSeconds()).hasValue(3600);
    }

    @Test
    void testStsPolicyAlternativeToRoleArn()
    {
        IcebergRestCatalogSigV4Config config = new IcebergRestCatalogSigV4Config()
                .setStsPolicy("{\"Version\":\"2012-10-17\",\"Statement\":[]}");

        assertThat(config.getStsPolicy()).hasValue("{\"Version\":\"2012-10-17\",\"Statement\":[]}");
        assertThat(config.getStsRoleArn()).isEmpty();
        assertThat(config.isStsRoleArnAndPolicyMutuallyExclusive()).isTrue();
    }

    @Test
    void testRoleArnAndPolicyMutuallyExclusive()
    {
        IcebergRestCatalogSigV4Config config = new IcebergRestCatalogSigV4Config()
                .setStsRoleArn("arn:aws:iam::123456789012:role/my-role")
                .setStsPolicy("{\"Version\":\"2012-10-17\",\"Statement\":[]}");

        assertThat(config.isStsRoleArnAndPolicyMutuallyExclusive()).isFalse();
    }
}
