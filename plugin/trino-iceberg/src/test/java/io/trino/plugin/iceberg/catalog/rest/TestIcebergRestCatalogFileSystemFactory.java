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
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY;
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
                S3FileIOProperties.ACCESS_KEY_ID, "test-access-key",
                S3FileIOProperties.SECRET_ACCESS_KEY, "test-secret-key",
                S3FileIOProperties.SESSION_TOKEN, "test-session-token");

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
                GCPProperties.GCS_OAUTH2_TOKEN, "ya29.test-token");

        factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties);

        ConnectorIdentity identity = capturedIdentity.get();
        assertThat(identity).isNotNull();
        assertThat(identity.getExtraCredentials())
                .containsEntry(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY, "ya29.test-token")
                .doesNotContainKey(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY)
                .doesNotContainKey(EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY);
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
                .put(GCPProperties.GCS_OAUTH2_TOKEN, "ya29.test-token")
                .put(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, "1700000000000")
                .put(GCPProperties.GCS_PROJECT_ID, "my-gcp-project")
                .buildOrThrow();

        factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties);

        ConnectorIdentity identity = capturedIdentity.get();
        assertThat(identity).isNotNull();
        assertThat(identity.getExtraCredentials())
                .containsEntry(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY, "ya29.test-token")
                .containsEntry(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY, "1700000000000")
                .containsEntry(EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY, "my-gcp-project");
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
                GCPProperties.GCS_OAUTH2_TOKEN, "ya29.test-token",
                GCPProperties.GCS_PROJECT_ID, "my-gcp-project");

        ConnectorIdentity originalIdentity = ConnectorIdentity.ofUser("test");
        factory.create(originalIdentity, fileIoProperties);

        ConnectorIdentity identity = capturedIdentity.get();
        assertThat(identity).isNotNull();
        assertThat(identity.getExtraCredentials()).isEmpty();
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
