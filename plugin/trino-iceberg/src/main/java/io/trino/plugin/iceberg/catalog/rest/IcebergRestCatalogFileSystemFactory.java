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
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.encryption.DualKeyEncryptionFileSystem;
import io.trino.filesystem.encryption.EncryptionEnforcingFileSystem;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.gcp.GCPProperties;

import java.util.Base64;
import java.util.Map;
import java.util.Optional;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static java.util.Objects.requireNonNull;

public class IcebergRestCatalogFileSystemFactory
        implements IcebergFileSystemFactory
{
    private static final String VENDED_S3_ACCESS_KEY = "s3.access-key-id";
    private static final String VENDED_S3_SECRET_KEY = "s3.secret-access-key";
    private static final String VENDED_S3_SESSION_TOKEN = "s3.session-token";

    private final TrinoFileSystemFactory fileSystemFactory;
    private final boolean vendedCredentialsEnabled;

    @Inject
    public IcebergRestCatalogFileSystemFactory(TrinoFileSystemFactory fileSystemFactory, IcebergRestCatalogConfig config)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.vendedCredentialsEnabled = config.isVendedCredentialsEnabled();
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity, Map<String, String> fileIoProperties)
    {
        TrinoFileSystem fileSystem = createBaseFileSystem(identity, fileIoProperties);
        if (vendedCredentialsEnabled) {
            fileSystem = applyVendedEncryptionKeys(fileSystem, fileIoProperties);
        }
        return fileSystem;
    }

    private TrinoFileSystem createBaseFileSystem(ConnectorIdentity identity, Map<String, String> fileIoProperties)
    {
        if (vendedCredentialsEnabled &&
                fileIoProperties.containsKey(VENDED_S3_ACCESS_KEY) &&
                fileIoProperties.containsKey(VENDED_S3_SECRET_KEY) &&
                fileIoProperties.containsKey(VENDED_S3_SESSION_TOKEN)) {
            // Do not include original credentials as they should not be used in vended mode
            ConnectorIdentity identityWithExtraCredentials = ConnectorIdentity.forUser(identity.getUser())
                    .withGroups(identity.getGroups())
                    .withPrincipal(identity.getPrincipal())
                    .withEnabledSystemRoles(identity.getEnabledSystemRoles())
                    .withConnectorRole(identity.getConnectorRole())
                    .withExtraCredentials(ImmutableMap.<String, String>builder()
                            .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, fileIoProperties.get(VENDED_S3_ACCESS_KEY))
                            .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, fileIoProperties.get(VENDED_S3_SECRET_KEY))
                            .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, fileIoProperties.get(VENDED_S3_SESSION_TOKEN))
                            .buildOrThrow())
                    .build();
            return fileSystemFactory.create(identityWithExtraCredentials);
        }

        return fileSystemFactory.create(identity);
    }

    private static TrinoFileSystem applyVendedEncryptionKeys(TrinoFileSystem fileSystem, Map<String, String> fileIoProperties)
    {
        // S3 SSE-C: customer-provided key, same key for reads and writes
        if (S3FileIOProperties.SSE_TYPE_CUSTOM.equals(fileIoProperties.get(S3FileIOProperties.SSE_TYPE))) {
            String sseKey = fileIoProperties.get(S3FileIOProperties.SSE_KEY);
            if (sseKey != null) {
                EncryptionKey encryptionKey = new EncryptionKey(Base64.getDecoder().decode(sseKey), "AES256");
                fileSystem = new EncryptionEnforcingFileSystem(fileSystem, encryptionKey);
            }
        }

        // GCS CSEK: supports separate encryption (write) and decryption (read) keys for key rotation
        String gcsEncryptionKey = fileIoProperties.get(GCPProperties.GCS_ENCRYPTION_KEY);
        String gcsDecryptionKey = fileIoProperties.get(GCPProperties.GCS_DECRYPTION_KEY);
        if (gcsEncryptionKey != null || gcsDecryptionKey != null) {
            Optional<EncryptionKey> encKey = Optional.ofNullable(gcsEncryptionKey)
                    .map(k -> new EncryptionKey(Base64.getDecoder().decode(k), "AES256"));
            Optional<EncryptionKey> decKey = Optional.ofNullable(gcsDecryptionKey)
                    .map(k -> new EncryptionKey(Base64.getDecoder().decode(k), "AES256"));
            fileSystem = new DualKeyEncryptionFileSystem(fileSystem, encKey, decKey);
        }

        return fileSystem;
    }
}
