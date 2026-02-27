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
package io.trino.filesystem.gcs;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;

import java.util.Base64;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_DECRYPTION_KEY_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_ENCRYPTION_KEY_PROPERTY;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class GcsFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final int readBlockSizeBytes;
    private final long writeBlockSizeBytes;
    private final int pageSize;
    private final int batchSize;
    private final ListeningExecutorService executorService;
    private final GcsStorageFactory storageFactory;

    @Inject
    public GcsFileSystemFactory(GcsFileSystemConfig config, GcsStorageFactory storageFactory)
    {
        this.readBlockSizeBytes = toIntExact(config.getReadBlockSize().toBytes());
        this.writeBlockSizeBytes = config.getWriteBlockSize().toBytes();
        this.pageSize = config.getPageSize();
        this.batchSize = config.getBatchSize();
        this.storageFactory = requireNonNull(storageFactory, "storageFactory is null");
        this.executorService = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("trino-filesystem-gcs-%S")));
    }

    @PreDestroy
    public void stop()
    {
        executorService.shutdownNow();
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        Optional<EncryptionKey> defaultEncryptionKey = extractEncryptionKey(identity, EXTRA_CREDENTIALS_GCS_ENCRYPTION_KEY_PROPERTY);
        Optional<EncryptionKey> defaultDecryptionKey = extractEncryptionKey(identity, EXTRA_CREDENTIALS_GCS_DECRYPTION_KEY_PROPERTY);
        return new GcsFileSystem(executorService, storageFactory.create(identity), readBlockSizeBytes, writeBlockSizeBytes, pageSize, batchSize, defaultEncryptionKey, defaultDecryptionKey);
    }

    private static Optional<EncryptionKey> extractEncryptionKey(ConnectorIdentity identity, String extraCredentialKey)
    {
        String base64Key = identity.getExtraCredentials().get(extraCredentialKey);
        if (base64Key == null) {
            return Optional.empty();
        }
        byte[] keyBytes = Base64.getDecoder().decode(base64Key);
        return Optional.of(new EncryptionKey(keyBytes, "AES256"));
    }
}
