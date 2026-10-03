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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.filesystem.s3.S3Context.S3SseContext;
import io.trino.filesystem.s3.S3FileSystemConfig.ObjectCannedAcl;
import io.trino.filesystem.s3.S3FileSystemConfig.S3SseType;
import io.trino.filesystem.s3.S3FileSystemConfig.StorageClassType;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestS3Context
{
    private static final S3Context CONTEXT = new S3Context(
            toIntExact(DataSize.of(5, MEGABYTE).toBytes()),
            false,
            S3SseContext.of(S3SseType.NONE, null, null),
            Optional.empty(),
            StorageClassType.STANDARD,
            ObjectCannedAcl.NONE);

    private static final ConnectorIdentity VENDED_CREDENTIALS_IDENTITY = ConnectorIdentity.forUser("test")
            .withExtraCredentials(ImmutableMap.of(
                    EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "access-key",
                    EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "secret-key",
                    EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "session-token"))
            .build();

    @Test
    void testWithoutVendedCredentials()
    {
        assertThat(CONTEXT.withCredentials(ConnectorIdentity.ofUser("test"))).isSameAs(CONTEXT);
    }

    @Test
    void testVendedCredentialsWithoutRefresherAreStatic()
    {
        assertThat(CONTEXT.withCredentials(VENDED_CREDENTIALS_IDENTITY).credentialsProviderOverride())
                .get()
                .isInstanceOf(StaticCredentialsProvider.class);
    }

    @Test
    void testVendedCredentialsWithRefresherAreRefreshable()
    {
        assertThat(CONTEXT.withCredentials(VENDED_CREDENTIALS_IDENTITY, VENDED_CREDENTIALS_IDENTITY::getExtraCredentials).credentialsProviderOverride())
                .get()
                .isInstanceOf(SupplierAwsCredentialsProvider.class);
    }

    @Test
    void testRefresherWithoutVendedCredentials()
    {
        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        assertThatThrownBy(() -> CONTEXT.withCredentials(identity, identity::getExtraCredentials))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Credentials refresher requires vended S3 credentials in extra credentials");
    }
}
