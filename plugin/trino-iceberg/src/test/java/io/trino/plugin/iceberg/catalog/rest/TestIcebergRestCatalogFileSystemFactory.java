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
import io.airlift.units.Duration;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.UriLocation;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.spi.NodeVersion;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.filesystem.azure.AzureFileSystemConstants.EXTRA_CREDENTIALS_AZURE_SAS_TOKEN_PREFIX;
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
            return new MockTrinoFileSystem();
        };

        IcebergRestCatalogFileSystemFactory factory = createFactory(delegate, true);

        Map<String, String> fileIoProperties = ImmutableMap.of(
                S3FileIOProperties.ACCESS_KEY_ID, "test-access-key",
                S3FileIOProperties.SECRET_ACCESS_KEY, "test-secret-key",
                S3FileIOProperties.SESSION_TOKEN, "test-session-token");

        factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties).newInputFile(Location.of("s3://bucket/path"));

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
            return new MockTrinoFileSystem();
        };

        IcebergRestCatalogFileSystemFactory factory = createFactory(delegate, true);

        Map<String, String> fileIoProperties = ImmutableMap.of(
                GCPProperties.GCS_OAUTH2_TOKEN, "ya29.test-token");

        factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties).newInputFile(Location.of("gs://bucket/path"));

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
            return new MockTrinoFileSystem();
        };

        IcebergRestCatalogFileSystemFactory factory = createFactory(delegate, true);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put(GCPProperties.GCS_OAUTH2_TOKEN, "ya29.test-token")
                .put(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, "1700000000000")
                .put(GCPProperties.GCS_PROJECT_ID, "my-gcp-project")
                .buildOrThrow();

        factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties).newInputFile(Location.of("gs://bucket/path"));

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

        IcebergRestCatalogFileSystemFactory factory = createFactory(delegate, false);

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
            return new MockTrinoFileSystem();
        };

        IcebergRestCatalogFileSystemFactory factory = createFactory(delegate, true);

        ConnectorIdentity originalIdentity = ConnectorIdentity.ofUser("test");
        factory.create(originalIdentity, ImmutableMap.of());

        ConnectorIdentity identity = capturedIdentity.get();
        assertThat(identity).isSameAs(originalIdentity);
    }

    @Test
    void testAzureVendedCredentialsWithSingleAccount()
    {
        AtomicReference<ConnectorIdentity> capturedIdentity = new AtomicReference<>();
        TrinoFileSystemFactory delegate = identity -> {
            capturedIdentity.set(identity);
            return new MockTrinoFileSystem();
        };

        IcebergRestCatalogFileSystemFactory factory = createFactory(delegate, true);

        Map<String, String> fileIoProperties = ImmutableMap.of(
                AzureProperties.ADLS_SAS_TOKEN_PREFIX + "mystorageaccount", "sv=2022-test-sas-token");

        factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties).newInputFile(Location.of("abfs://container@mystorageaccount.dfs.core.windows.net/some/path/file"));

        ConnectorIdentity identity = capturedIdentity.get();
        assertThat(identity).isNotNull();
        assertThat(identity.getExtraCredentials())
                .containsEntry(EXTRA_CREDENTIALS_AZURE_SAS_TOKEN_PREFIX + "mystorageaccount", "sv=2022-test-sas-token");
    }

    @Test
    void testAzureVendedCredentialsWithMultipleAccounts()
    {
        AtomicReference<ConnectorIdentity> capturedIdentity = new AtomicReference<>();
        TrinoFileSystemFactory delegate = identity -> {
            capturedIdentity.set(identity);
            return new MockTrinoFileSystem();
        };

        IcebergRestCatalogFileSystemFactory factory = createFactory(delegate, true);

        Map<String, String> fileIoProperties = ImmutableMap.of(
                AzureProperties.ADLS_SAS_TOKEN_PREFIX + "account1", "sas-token-1",
                AzureProperties.ADLS_SAS_TOKEN_PREFIX + "account2", "sas-token-2");

        factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties).newInputFile(Location.of("abfs://container@account1.dfs.core.windows.net/some/path/file"));

        ConnectorIdentity identity = capturedIdentity.get();
        assertThat(identity).isNotNull();
        assertThat(identity.getExtraCredentials())
                .containsEntry(EXTRA_CREDENTIALS_AZURE_SAS_TOKEN_PREFIX + "account1", "sas-token-1")
                .containsEntry(EXTRA_CREDENTIALS_AZURE_SAS_TOKEN_PREFIX + "account2", "sas-token-2");
    }

    @Test
    void testAzureVendedCredentialsWithExpiration()
    {
        AtomicReference<ConnectorIdentity> capturedIdentity = new AtomicReference<>();
        TrinoFileSystemFactory delegate = identity -> {
            capturedIdentity.set(identity);
            return new MockTrinoFileSystem();
        };

        IcebergRestCatalogFileSystemFactory factory = createFactory(delegate, true);

        Map<String, String> fileIoProperties = ImmutableMap.of(AzureProperties.ADLS_SAS_TOKEN_PREFIX + "mystorageaccount", "sv=2022-test-sas-token");

        factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties).newInputFile(Location.of("abfs://container@mystorageaccount.dfs.core.windows.net/some/path/file"));

        ConnectorIdentity identity = capturedIdentity.get();
        assertThat(identity).isNotNull();
        assertThat(identity.getExtraCredentials())
                .containsEntry(EXTRA_CREDENTIALS_AZURE_SAS_TOKEN_PREFIX + "mystorageaccount", "sv=2022-test-sas-token");
    }

    @Test
    void testAzureVendedCredentialsDisabled()
    {
        AtomicReference<ConnectorIdentity> capturedIdentity = new AtomicReference<>();
        TrinoFileSystemFactory delegate = identity -> {
            capturedIdentity.set(identity);
            return new MockTrinoFileSystem();
        };

        IcebergRestCatalogFileSystemFactory factory = createFactory(delegate, false);

        Map<String, String> fileIoProperties = ImmutableMap.of(
                AzureProperties.ADLS_SAS_TOKEN_PREFIX + "mystorageaccount", "sv=2022-test-sas-token");

        ConnectorIdentity originalIdentity = ConnectorIdentity.ofUser("test");
        factory.create(originalIdentity, fileIoProperties);

        ConnectorIdentity identity = capturedIdentity.get();
        assertThat(identity).isNotNull();
        assertThat(identity.getExtraCredentials()).isEmpty();
    }

    private static IcebergRestCatalogFileSystemFactory createFactory(TrinoFileSystemFactory delegate, boolean vendedCredentialsEnabled)
    {
        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setBaseUri("http://localhost")
                .setVendedCredentialsEnabled(vendedCredentialsEnabled);
        IcebergRestCatalogPropertiesProvider catalogPropertiesProvider = new IcebergRestCatalogPropertiesProvider(
                config,
                new NoneSecurityProperties(),
                new NodeVersion("test"));
        return new IcebergRestCatalogFileSystemFactory(
                delegate,
                config,
                catalogPropertiesProvider);
    }

    private static class MockTrinoFileSystem
            implements TrinoFileSystem
    {
        @Override
        public TrinoInputFile newEncryptedInputFile(Location location, long length, EncryptionKey key)
        {
            return TrinoFileSystem.super.newEncryptedInputFile(location, length, key);
        }

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            return null;
        }

        @Override
        public TrinoInputFile newEncryptedInputFile(Location location, EncryptionKey key)
        {
            return TrinoFileSystem.super.newEncryptedInputFile(location, key);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            return null;
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            return null;
        }

        @Override
        public TrinoInputFile newEncryptedInputFile(Location location, long length, Instant lastModified, EncryptionKey key)
        {
            return TrinoFileSystem.super.newEncryptedInputFile(location, length, lastModified, key);
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            return null;
        }

        @Override
        public TrinoOutputFile newEncryptedOutputFile(Location location, EncryptionKey key)
        {
            return null;
        }

        @Override
        public void deleteFile(Location location)
        {
        }

        @Override
        public void deleteFiles(Collection<Location> locations)
        {
        }

        @Override
        public void deleteDirectory(Location location)
        {
        }

        @Override
        public void renameFile(Location source, Location target)
        {
        }

        @Override
        public FileIterator listFiles(Location location)
        {
            return null;
        }

        @Override
        public Optional<Boolean> directoryExists(Location location)
        {
            return Optional.empty();
        }

        @Override
        public void createDirectory(Location location)
        {
        }

        @Override
        public void renameDirectory(Location source, Location target)
        {
        }

        @Override
        public Set<Location> listDirectories(Location location)
        {
            return Set.of();
        }

        @Override
        public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
        {
            return Optional.empty();
        }

        @Override
        public Optional<UriLocation> preSignedUri(Location location, Duration ttl)
                throws IOException
        {
            return Optional.empty();
        }

        @Override
        public Optional<UriLocation> encryptedPreSignedUri(Location location, Duration ttl, EncryptionKey key)
        {
            return Optional.empty();
        }
    }
}
