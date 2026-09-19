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

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.cloud.storage.Storage.BlobTargetOption.encryptionKey;
import static com.google.cloud.storage.Storage.BlobTargetOption.kmsKeyName;
import static io.trino.filesystem.encryption.EncryptionKey.randomAes256;
import static io.trino.filesystem.gcs.GcsFileSystemConfig.GcsSseType.KMS;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_CUSTOMER_DECRYPTION_KEY_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_CUSTOMER_ENCRYPTION_KEY_PROPERTY;
import static io.trino.filesystem.gcs.GcsUtils.encodedKey;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.lang.reflect.Proxy.newProxyInstance;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class TestGcsOutputFile
{
    private static final GcsLocation LOCATION = new GcsLocation(Location.of("gs://bucket/key"));

    @Test
    void testKmsEncryption()
            throws Exception
    {
        AtomicReference<BlobTargetOption[]> options = new AtomicReference<>();
        GcsOutputFile outputFile = new GcsOutputFile(
                LOCATION,
                capturingStorage(options),
                16,
                Optional.of("kmsKeyName"),
                Optional.empty());

        outputFile.createOrOverwrite(new byte[0]);

        assertThat(options.get()).containsExactly(kmsKeyName("kmsKeyName"));
    }

    @Test
    void testCustomerEncryption()
            throws Exception
    {
        EncryptionKey key = randomAes256();
        AtomicReference<BlobTargetOption[]> options = new AtomicReference<>();
        GcsOutputFile outputFile = new GcsOutputFile(
                LOCATION,
                capturingStorage(options),
                16,
                Optional.empty(),
                Optional.of(key));

        outputFile.createOrOverwrite(new byte[0]);

        assertThat(options.get()).containsExactly(encryptionKey(encodedKey(key)));
    }

    @Test
    void testStreamingEncryption()
            throws Exception
    {
        AtomicReference<BlobWriteOption[]> options = new AtomicReference<>();
        GcsOutputFile outputFile = new GcsOutputFile(
                LOCATION,
                capturingStreamingStorage(options),
                16,
                Optional.of("kmsKeyName"),
                Optional.empty());

        outputFile.create(newSimpleAggregatedMemoryContext()).close();

        assertThat(options.get()).containsExactly(BlobWriteOption.doesNotExist(), BlobWriteOption.kmsKeyName("kmsKeyName"));
    }

    @Test
    void testEncryptionOptionsAreMutuallyExclusive()
    {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new GcsOutputFile(
                        LOCATION,
                        capturingStorage(new AtomicReference<>()),
                        16,
                        Optional.of("kmsKeyName"),
                        Optional.of(randomAes256())))
                .withMessage("KMS key and customer-supplied encryption key cannot both be set");
    }

    @Test
    void testIdentityCustomerEncryptionKeyOverridesKmsEncryption()
            throws Exception
    {
        EncryptionKey key = randomAes256();
        AtomicReference<BlobTargetOption[]> options = new AtomicReference<>();
        Storage storage = capturingStorage(options);
        GcsFileSystemConfig config = new GcsFileSystemConfig()
                .setSseType(KMS)
                .setSseKmsKeyName("kmsKeyName");
        GcsStorageFactory storageFactory = new GcsStorageFactory(config, (_, _) -> {})
        {
            @Override
            public Storage create(ConnectorIdentity identity)
            {
                return storage;
            }
        };
        GcsFileSystemFactory fileSystemFactory = new GcsFileSystemFactory(config, storageFactory);
        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(ImmutableMap.of(
                        EXTRA_CREDENTIALS_GCS_CUSTOMER_ENCRYPTION_KEY_PROPERTY, encodedKey(key),
                        EXTRA_CREDENTIALS_GCS_CUSTOMER_DECRYPTION_KEY_PROPERTY, encodedKey(key)))
                .build();

        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(identity);
            fileSystem.newOutputFile(LOCATION.location()).createOrOverwrite(new byte[0]);
        }
        finally {
            fileSystemFactory.stop();
        }

        assertThat(options.get()).containsExactly(encryptionKey(encodedKey(key)));
    }

    @Test
    void testIdentityCustomerEncryptionKeyCanBeUsedWithoutDecryptionKey()
            throws Exception
    {
        EncryptionKey key = randomAes256();
        AtomicReference<BlobTargetOption[]> options = new AtomicReference<>();
        Storage storage = capturingStorage(options);
        GcsFileSystemConfig config = new GcsFileSystemConfig()
                .setSseType(KMS)
                .setSseKmsKeyName("kmsKeyName");
        GcsStorageFactory storageFactory = new GcsStorageFactory(config, (_, _) -> {})
        {
            @Override
            public Storage create(ConnectorIdentity identity)
            {
                return storage;
            }
        };
        GcsFileSystemFactory fileSystemFactory = new GcsFileSystemFactory(config, storageFactory);
        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(ImmutableMap.of(
                        EXTRA_CREDENTIALS_GCS_CUSTOMER_ENCRYPTION_KEY_PROPERTY, encodedKey(key)))
                .build();

        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(identity);
            fileSystem.newOutputFile(LOCATION.location()).createOrOverwrite(new byte[0]);
        }
        finally {
            fileSystemFactory.stop();
        }

        assertThat(options.get()).containsExactly(encryptionKey(encodedKey(key)));
    }

    private static Storage capturingStorage(AtomicReference<BlobTargetOption[]> options)
    {
        return (Storage) newProxyInstance(
                TestGcsOutputFile.class.getClassLoader(),
                new Class<?>[] {Storage.class},
                (_, method, arguments) -> {
                    if (method.getName().equals("create") && arguments.length == 3 && arguments[0] instanceof BlobInfo) {
                        options.set((BlobTargetOption[]) arguments[2]);
                        return null;
                    }
                    throw new AssertionError("Unexpected Storage access: " + method);
                });
    }

    private static Storage capturingStreamingStorage(AtomicReference<BlobWriteOption[]> options)
    {
        WriteChannel writeChannel = (WriteChannel) newProxyInstance(
                TestGcsOutputFile.class.getClassLoader(),
                new Class<?>[] {WriteChannel.class},
                (_, method, _) -> {
                    if (method.getName().equals("setChunkSize") || method.getName().equals("close")) {
                        return null;
                    }
                    throw new AssertionError("Unexpected WriteChannel access: " + method);
                });
        return (Storage) newProxyInstance(
                TestGcsOutputFile.class.getClassLoader(),
                new Class<?>[] {Storage.class},
                (_, method, arguments) -> {
                    if (method.getName().equals("writer") && arguments.length == 2 && arguments[0] instanceof BlobInfo) {
                        options.set((BlobWriteOption[]) arguments[1]);
                        return writeChannel;
                    }
                    throw new AssertionError("Unexpected Storage access: " + method);
                });
    }
}
