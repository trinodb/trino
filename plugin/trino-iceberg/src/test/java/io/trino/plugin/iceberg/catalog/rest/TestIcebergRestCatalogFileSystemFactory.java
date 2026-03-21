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
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.encryption.DualKeyEncryptionFileSystem;
import io.trino.filesystem.encryption.EncryptionEnforcingFileSystem;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergRestCatalogFileSystemFactory
{
    // 32-byte AES-256 key encoded in Base64
    private static final String BASE64_KEY_A = Base64.getEncoder().encodeToString(new byte[32]);
    private static final String BASE64_KEY_B;

    static {
        byte[] keyB = new byte[32];
        keyB[0] = 1;
        BASE64_KEY_B = Base64.getEncoder().encodeToString(keyB);
    }

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
    void testS3SseCVendedEncryptionKey()
    {
        TrinoFileSystemFactory delegate = identity -> new NoOpTrinoFileSystem();

        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setBaseUri("http://localhost")
                .setVendedCredentialsEnabled(true);
        IcebergRestCatalogFileSystemFactory factory = new IcebergRestCatalogFileSystemFactory(delegate, config);

        Map<String, String> fileIoProperties = ImmutableMap.of(
                S3FileIOProperties.SSE_TYPE, S3FileIOProperties.SSE_TYPE_CUSTOM,
                S3FileIOProperties.SSE_KEY, BASE64_KEY_A);

        TrinoFileSystem fileSystem = factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties);

        assertThat(fileSystem).isInstanceOf(EncryptionEnforcingFileSystem.class);
    }

    @Test
    void testS3SseKmsIsNotWrapped()
    {
        TrinoFileSystemFactory delegate = identity -> new NoOpTrinoFileSystem();

        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setBaseUri("http://localhost")
                .setVendedCredentialsEnabled(true);
        IcebergRestCatalogFileSystemFactory factory = new IcebergRestCatalogFileSystemFactory(delegate, config);

        // SSE-KMS uses server-managed keys; no client-side wrapping needed
        Map<String, String> fileIoProperties = ImmutableMap.of(
                S3FileIOProperties.SSE_TYPE, S3FileIOProperties.SSE_TYPE_KMS,
                S3FileIOProperties.SSE_KEY, "arn:aws:kms:us-east-1:123456789:key/test-key");

        TrinoFileSystem fileSystem = factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties);

        assertThat(fileSystem).isNotInstanceOf(EncryptionEnforcingFileSystem.class);
    }

    @Test
    void testGcsCsekBothKeys()
    {
        TrinoFileSystemFactory delegate = identity -> new NoOpTrinoFileSystem();

        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setBaseUri("http://localhost")
                .setVendedCredentialsEnabled(true);
        IcebergRestCatalogFileSystemFactory factory = new IcebergRestCatalogFileSystemFactory(delegate, config);

        // GCS key rotation: different encryption and decryption keys
        Map<String, String> fileIoProperties = ImmutableMap.of(
                GCPProperties.GCS_ENCRYPTION_KEY, BASE64_KEY_A,
                GCPProperties.GCS_DECRYPTION_KEY, BASE64_KEY_B);

        TrinoFileSystem fileSystem = factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties);

        assertThat(fileSystem).isInstanceOf(DualKeyEncryptionFileSystem.class);
    }

    @Test
    void testGcsCsekEncryptionKeyOnly()
    {
        TrinoFileSystemFactory delegate = identity -> new NoOpTrinoFileSystem();

        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setBaseUri("http://localhost")
                .setVendedCredentialsEnabled(true);
        IcebergRestCatalogFileSystemFactory factory = new IcebergRestCatalogFileSystemFactory(delegate, config);

        // Only encryption key: new writes are encrypted, reads are unencrypted
        Map<String, String> fileIoProperties = ImmutableMap.of(
                GCPProperties.GCS_ENCRYPTION_KEY, BASE64_KEY_A);

        TrinoFileSystem fileSystem = factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties);

        assertThat(fileSystem).isInstanceOf(DualKeyEncryptionFileSystem.class);
    }

    @Test
    void testGcsCsekDecryptionKeyOnly()
    {
        TrinoFileSystemFactory delegate = identity -> new NoOpTrinoFileSystem();

        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setBaseUri("http://localhost")
                .setVendedCredentialsEnabled(true);
        IcebergRestCatalogFileSystemFactory factory = new IcebergRestCatalogFileSystemFactory(delegate, config);

        // Only decryption key: reads use the key, writes are unencrypted
        Map<String, String> fileIoProperties = ImmutableMap.of(
                GCPProperties.GCS_DECRYPTION_KEY, BASE64_KEY_B);

        TrinoFileSystem fileSystem = factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties);

        assertThat(fileSystem).isInstanceOf(DualKeyEncryptionFileSystem.class);
    }

    @Test
    void testEncryptionKeysNotAppliedWhenVendedCredentialsDisabled()
    {
        TrinoFileSystemFactory delegate = identity -> new NoOpTrinoFileSystem();

        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setBaseUri("http://localhost")
                .setVendedCredentialsEnabled(false);
        IcebergRestCatalogFileSystemFactory factory = new IcebergRestCatalogFileSystemFactory(delegate, config);

        Map<String, String> fileIoProperties = ImmutableMap.of(
                S3FileIOProperties.SSE_TYPE, S3FileIOProperties.SSE_TYPE_CUSTOM,
                S3FileIOProperties.SSE_KEY, BASE64_KEY_A,
                GCPProperties.GCS_ENCRYPTION_KEY, BASE64_KEY_A);

        TrinoFileSystem fileSystem = factory.create(ConnectorIdentity.ofUser("test"), fileIoProperties);

        assertThat(fileSystem)
                .isNotInstanceOf(EncryptionEnforcingFileSystem.class)
                .isNotInstanceOf(DualKeyEncryptionFileSystem.class);
    }

    @Test
    void testNoVendedCredentials()
    {
        AtomicReference<ConnectorIdentity> capturedIdentity = new AtomicReference<>();
        TrinoFileSystemFactory delegate = identity -> {
            capturedIdentity.set(identity);
            return new NoOpTrinoFileSystem();
        };

        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setBaseUri("http://localhost")
                .setVendedCredentialsEnabled(true);
        IcebergRestCatalogFileSystemFactory factory = new IcebergRestCatalogFileSystemFactory(delegate, config);

        ConnectorIdentity originalIdentity = ConnectorIdentity.ofUser("test");
        factory.create(originalIdentity, ImmutableMap.of());

        assertThat(capturedIdentity.get()).isSameAs(originalIdentity);
    }

    private static class NoOpTrinoFileSystem
            implements TrinoFileSystem
    {
        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, java.time.Instant lastModified)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteDirectory(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameFile(Location source, Location target)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileIterator listFiles(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Boolean> directoryExists(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createDirectory(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameDirectory(Location source, Location target)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Location> listDirectories(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
        {
            throw new UnsupportedOperationException();
        }
    }
}
