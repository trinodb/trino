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
package io.trino.spooling.filesystem;

import com.google.common.collect.ImmutableMap;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.Node;
import io.trino.spi.spool.SpoolingManager;
import io.trino.spi.spool.SpoolingManagerContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestFileSystemSpoolingModule
{
    private static final Map<String, String> S3_WEB_IDENTITY_WITH_VALIDATION = ImmutableMap.<String, String>builder()
            .put("fs.s3.enabled", "true")
            .put("fs.location", "s3://bucket/segments/")
            .put("s3.region", "us-east-1")
            .put("s3.auth-type", "WEB_IDENTITY")
            .put("s3.presigned-uri.lifetime-validation.enabled", "true")
            .buildOrThrow();

    @Test
    void testDefaultDurationsRejectedForWebIdentity()
    {
        assertThatThrownBy(() -> createSpoolingManager(S3_WEB_IDENTITY_WITH_VALIDATION))
                .hasMessageContaining("fs.segment.direct.ttl (1.00h) must be no longer than s3.role-session-duration (1.00h) minus 5.00m " +
                        "when s3.presigned-uri.lifetime-validation.enabled is set");
    }

    @Test
    void testDefaultDurationsRejectedForIamRole()
    {
        assertThatThrownBy(() -> createSpoolingManager(with(S3_WEB_IDENTITY_WITH_VALIDATION, ImmutableMap.of(
                "s3.auth-type", "IAM_ROLE",
                "s3.iam-role", "arn:aws:iam::123456789012:role/test"))))
                .hasMessageContaining("fs.segment.direct.ttl (1.00h) must be no longer than s3.role-session-duration (1.00h) minus 5.00m");
    }

    @Test
    void testTtlPlusMarginLongerThanSessionDurationRejected()
    {
        assertThatThrownBy(() -> createSpoolingManager(with(S3_WEB_IDENTITY_WITH_VALIDATION, ImmutableMap.of(
                "fs.segment.direct.ttl", "56m",
                "s3.role-session-duration", "1h"))))
                .hasMessageContaining("fs.segment.direct.ttl (56.00m) must be no longer than s3.role-session-duration (1.00h) minus 5.00m");
    }

    @Test
    void testTtlPlusMarginNotLongerThanSessionDurationAccepted()
    {
        assertThat(createSpoolingManager(with(S3_WEB_IDENTITY_WITH_VALIDATION, ImmutableMap.of(
                "fs.segment.direct.ttl", "55m",
                "s3.role-session-duration", "1h"))))
                .isInstanceOf(FileSystemSpoolingManager.class);
        assertThat(createSpoolingManager(with(S3_WEB_IDENTITY_WITH_VALIDATION, ImmutableMap.of(
                "fs.segment.direct.ttl", "54m",
                "s3.role-session-duration", "1h"))))
                .isInstanceOf(FileSystemSpoolingManager.class);
        assertThat(createSpoolingManager(with(S3_WEB_IDENTITY_WITH_VALIDATION, ImmutableMap.of(
                "s3.role-session-duration", "2h"))))
                .isInstanceOf(FileSystemSpoolingManager.class);
    }

    @Test
    void testDefaultDurationsAcceptedWithoutValidation()
    {
        assertThat(createSpoolingManager(with(S3_WEB_IDENTITY_WITH_VALIDATION, ImmutableMap.of(
                "s3.presigned-uri.lifetime-validation.enabled", "false"))))
                .isInstanceOf(FileSystemSpoolingManager.class);
    }

    @Test
    void testDefaultDurationsAcceptedForStaticCredentials()
    {
        assertThat(createSpoolingManager(with(S3_WEB_IDENTITY_WITH_VALIDATION, ImmutableMap.of(
                "s3.auth-type", "DEFAULT",
                "s3.aws-access-key", "access",
                "s3.aws-secret-key", "secret"))))
                .isInstanceOf(FileSystemSpoolingManager.class);
    }

    @Test
    void testDefaultCredentialsChainRejected()
    {
        assertThatThrownBy(() -> createSpoolingManager(with(S3_WEB_IDENTITY_WITH_VALIDATION, ImmutableMap.of(
                "s3.auth-type", "DEFAULT"))))
                .hasMessageContaining("s3.presigned-uri.lifetime-validation.enabled requires s3.auth-type=IAM_ROLE, s3.auth-type=WEB_IDENTITY, or s3.aws-access-key and s3.aws-secret-key");
    }

    private static Map<String, String> with(Map<String, String> base, Map<String, String> overrides)
    {
        return ImmutableMap.<String, String>builder()
                .putAll(base)
                .putAll(overrides)
                .buildKeepingLast();
    }

    private static SpoolingManager createSpoolingManager(Map<String, String> properties)
    {
        return new FileSystemSpoolingManagerFactory().create(properties, new SpoolingManagerContext()
        {
            @Override
            public OpenTelemetry getOpenTelemetry()
            {
                return OpenTelemetry.noop();
            }

            @Override
            public Tracer getTracer()
            {
                return OpenTelemetry.noop().getTracer("test");
            }

            @Override
            public boolean isCoordinator()
            {
                return false;
            }

            @Override
            public Node getCurrentNode()
            {
                return new TestingNode("test");
            }
        });
    }
}
