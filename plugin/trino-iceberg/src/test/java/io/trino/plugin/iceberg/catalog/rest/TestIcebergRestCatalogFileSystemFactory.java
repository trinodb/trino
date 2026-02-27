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

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_DECRYPTION_KEY_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_ENCRYPTION_KEY_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_NO_AUTH_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_SERVICE_HOST_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_USER_PROJECT_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergRestCatalogFileSystemFactory
{
    @Test
    void testS3VendedCredentials()
    {
        AtomicReference<ConnectorIdentity> capturedIdentity = new AtomicReference<>();
        TrinoFileSystemFactory delegate = identity -> {
            capturedIdentity.set(identity);
            return null;
        };

        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setBaseUri("http://localhost")
                .setVendedCredentialsEnabled(true);
        IcebergRestCatalogFileSystemFactory factory = new IcebergRestCatalogFileSystemFactory(delegate, config);

        Map<String, String> fileIoProperties = ImmutableMap.of(
                "s3.access-key-id", "test-access-key",
                "s3.secret-access-key", "test-secret-key",
                "s3.session-token", "test-session-token");

        factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties);

        ConnectorIdentity identity = capturedIdentity.get();
        assertThat(identity).isNotNull();
        assertThat(identity.getExtraCredentials())
                .containsEntry(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "test-access-key")
                .containsEntry(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "test-secret-key")
                .containsEntry(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "test-session-token");
    }

    @Test
    void testGcsVendedCredentialsWithOAuthTokenOnly()
    {
        AtomicReference<ConnectorIdentity> capturedIdentity = new AtomicReference<>();
        TrinoFileSystemFactory delegate = identity -> {
            capturedIdentity.set(identity);
            return null;
        };

        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setBaseUri("http://localhost")
                .setVendedCredentialsEnabled(true);
        IcebergRestCatalogFileSystemFactory factory = new IcebergRestCatalogFileSystemFactory(delegate, config);

        Map<String, String> fileIoProperties = ImmutableMap.of(
                "gcs.oauth2.token", "ya29.test-token");

        factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties);

        ConnectorIdentity identity = capturedIdentity.get();
        assertThat(identity).isNotNull();
        assertThat(identity.getExtraCredentials())
                .containsEntry(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY, "ya29.test-token")
                .doesNotContainKey(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY)
                .doesNotContainKey(EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY)
                .doesNotContainKey(EXTRA_CREDENTIALS_GCS_SERVICE_HOST_PROPERTY);
    }

    @Test
    void testGcsVendedCredentialsWithAllProperties()
    {
        AtomicReference<ConnectorIdentity> capturedIdentity = new AtomicReference<>();
        TrinoFileSystemFactory delegate = identity -> {
            capturedIdentity.set(identity);
            return null;
        };

        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setBaseUri("http://localhost")
                .setVendedCredentialsEnabled(true);
        IcebergRestCatalogFileSystemFactory factory = new IcebergRestCatalogFileSystemFactory(delegate, config);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("gcs.oauth2.token", "ya29.test-token")
                .put("gcs.oauth2.token-expires-at", "1700000000000")
                .put("gcs.project-id", "my-gcp-project")
                .put("gcs.service.host", "https://custom-storage.googleapis.com")
                .put("gcs.user-project", "billing-project")
                .put("gcs.encryption-key", "dGVzdC1lbmNyeXB0aW9uLWtleS0xMjM0NTY3OA==")
                .put("gcs.decryption-key", "dGVzdC1kZWNyeXB0aW9uLWtleS0xMjM0NTY3OA==")
                .buildOrThrow();

        factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties);

        ConnectorIdentity identity = capturedIdentity.get();
        assertThat(identity).isNotNull();
        assertThat(identity.getExtraCredentials())
                .containsEntry(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY, "ya29.test-token")
                .containsEntry(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY, "1700000000000")
                .containsEntry(EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY, "my-gcp-project")
                .containsEntry(EXTRA_CREDENTIALS_GCS_SERVICE_HOST_PROPERTY, "https://custom-storage.googleapis.com")
                .containsEntry(EXTRA_CREDENTIALS_GCS_USER_PROJECT_PROPERTY, "billing-project")
                .containsEntry(EXTRA_CREDENTIALS_GCS_ENCRYPTION_KEY_PROPERTY, "dGVzdC1lbmNyeXB0aW9uLWtleS0xMjM0NTY3OA==")
                .containsEntry(EXTRA_CREDENTIALS_GCS_DECRYPTION_KEY_PROPERTY, "dGVzdC1kZWNyeXB0aW9uLWtleS0xMjM0NTY3OA==");
    }

    @Test
    void testGcsVendedNoAuth()
    {
        AtomicReference<ConnectorIdentity> capturedIdentity = new AtomicReference<>();
        TrinoFileSystemFactory delegate = identity -> {
            capturedIdentity.set(identity);
            return null;
        };

        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setBaseUri("http://localhost")
                .setVendedCredentialsEnabled(true);
        IcebergRestCatalogFileSystemFactory factory = new IcebergRestCatalogFileSystemFactory(delegate, config);

        Map<String, String> fileIoProperties = ImmutableMap.of(
                "gcs.no-auth", "true",
                "gcs.project-id", "public-project");

        factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties);

        ConnectorIdentity identity = capturedIdentity.get();
        assertThat(identity).isNotNull();
        assertThat(identity.getExtraCredentials())
                .containsEntry(EXTRA_CREDENTIALS_GCS_NO_AUTH_PROPERTY, "true")
                .containsEntry(EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY, "public-project")
                .doesNotContainKey(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY);
    }

    @Test
    void testGcsVendedEncryptionKeys()
    {
        AtomicReference<ConnectorIdentity> capturedIdentity = new AtomicReference<>();
        TrinoFileSystemFactory delegate = identity -> {
            capturedIdentity.set(identity);
            return null;
        };

        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setBaseUri("http://localhost")
                .setVendedCredentialsEnabled(true);
        IcebergRestCatalogFileSystemFactory factory = new IcebergRestCatalogFileSystemFactory(delegate, config);

        Map<String, String> fileIoProperties = ImmutableMap.of(
                "gcs.encryption-key", "dGVzdC1lbmNyeXB0aW9uLWtleS0xMjM0NTY3OA==",
                "gcs.decryption-key", "dGVzdC1kZWNyeXB0aW9uLWtleS0xMjM0NTY3OA==");

        factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties);

        ConnectorIdentity identity = capturedIdentity.get();
        assertThat(identity).isNotNull();
        assertThat(identity.getExtraCredentials())
                .containsEntry(EXTRA_CREDENTIALS_GCS_ENCRYPTION_KEY_PROPERTY, "dGVzdC1lbmNyeXB0aW9uLWtleS0xMjM0NTY3OA==")
                .containsEntry(EXTRA_CREDENTIALS_GCS_DECRYPTION_KEY_PROPERTY, "dGVzdC1kZWNyeXB0aW9uLWtleS0xMjM0NTY3OA==");
    }

    @Test
    void testGcsVendedCredentialsDisabled()
    {
        AtomicReference<ConnectorIdentity> capturedIdentity = new AtomicReference<>();
        TrinoFileSystemFactory delegate = identity -> {
            capturedIdentity.set(identity);
            return null;
        };

        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setBaseUri("http://localhost")
                .setVendedCredentialsEnabled(false);
        IcebergRestCatalogFileSystemFactory factory = new IcebergRestCatalogFileSystemFactory(delegate, config);

        Map<String, String> fileIoProperties = ImmutableMap.of(
                "gcs.oauth2.token", "ya29.test-token",
                "gcs.project-id", "my-gcp-project");

        ConnectorIdentity originalIdentity = ConnectorIdentity.ofUser("test");
        factory.create(originalIdentity, fileIoProperties);

        ConnectorIdentity identity = capturedIdentity.get();
        assertThat(identity).isNotNull();
        assertThat(identity.getExtraCredentials()).isEmpty();
    }

    @Test
    void testGcsVendedUserProjectOnly()
    {
        AtomicReference<ConnectorIdentity> capturedIdentity = new AtomicReference<>();
        TrinoFileSystemFactory delegate = identity -> {
            capturedIdentity.set(identity);
            return null;
        };

        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setBaseUri("http://localhost")
                .setVendedCredentialsEnabled(true);
        IcebergRestCatalogFileSystemFactory factory = new IcebergRestCatalogFileSystemFactory(delegate, config);

        Map<String, String> fileIoProperties = ImmutableMap.of(
                "gcs.user-project", "billing-project");

        factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties);

        ConnectorIdentity identity = capturedIdentity.get();
        assertThat(identity).isNotNull();
        assertThat(identity.getExtraCredentials())
                .containsEntry(EXTRA_CREDENTIALS_GCS_USER_PROJECT_PROPERTY, "billing-project")
                .doesNotContainKey(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY)
                .doesNotContainKey(EXTRA_CREDENTIALS_GCS_NO_AUTH_PROPERTY);
    }

    @Test
    void testNoVendedCredentialsInProperties()
    {
        AtomicReference<ConnectorIdentity> capturedIdentity = new AtomicReference<>();
        TrinoFileSystemFactory delegate = identity -> {
            capturedIdentity.set(identity);
            return null;
        };

        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setBaseUri("http://localhost")
                .setVendedCredentialsEnabled(true);
        IcebergRestCatalogFileSystemFactory factory = new IcebergRestCatalogFileSystemFactory(delegate, config);

        ConnectorIdentity originalIdentity = ConnectorIdentity.ofUser("test");
        factory.create(originalIdentity, ImmutableMap.of());

        ConnectorIdentity identity = capturedIdentity.get();
        assertThat(identity).isSameAs(originalIdentity);
    }
}
