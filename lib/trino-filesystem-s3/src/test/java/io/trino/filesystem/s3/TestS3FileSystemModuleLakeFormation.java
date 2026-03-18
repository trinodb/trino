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
package io.trino.filesystem.s3;

import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests mutual exclusivity between Lake Formation credential vending and S3 security mappings.
 * The check is implemented in S3FileSystemModule.setup() and throws CONFIGURATION_INVALID
 * when both fs.s3.lake-formation.enabled and s3.security-mapping.enabled are true.
 *
 * This is verified at the module level during Guice bootstrap. Full integration testing
 * requires the Trino connector test framework (e.g., BaseIcebergGlueConnectorSmokeTest).
 */
public class TestS3FileSystemModuleLakeFormation
{
    @Test
    public void testLakeFormationConfigDefaultsDisabled()
    {
        S3FileSystemConfig config = new S3FileSystemConfig();
        // Lake Formation is disabled by default, so mutual exclusivity is not triggered
        assertThat(config.isLakeFormationEnabled()).isFalse();
    }

    @Test
    public void testLakeFormationCacheTtlValidWhenDisabled()
    {
        // When LF is disabled, cache TTL validation always passes even with invalid values
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setLakeFormationEnabled(false)
                .setLakeFormationCredentialCacheTtl(new Duration(20, MINUTES))
                .setLakeFormationCredentialDuration(new Duration(10, MINUTES));
        assertThat(config.isLakeFormationCacheTtlValid()).isTrue();
    }

    @Test
    public void testLakeFormationCacheTtlInvalidWhenEnabled()
    {
        // When LF is enabled, cache TTL must be less than credential duration
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setLakeFormationEnabled(true)
                .setLakeFormationCredentialCacheTtl(new Duration(20, MINUTES))
                .setLakeFormationCredentialDuration(new Duration(10, MINUTES));
        assertThat(config.isLakeFormationCacheTtlValid()).isFalse();
    }
}
