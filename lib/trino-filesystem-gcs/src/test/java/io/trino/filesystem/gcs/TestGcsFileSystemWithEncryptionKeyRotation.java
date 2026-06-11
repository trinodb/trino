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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.Base64;

import static io.trino.filesystem.encryption.EncryptionKey.randomAes256;
import static io.trino.filesystem.gcs.GcsUtils.encodedKey;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Does not extend AbstractTestGcsFileSystem because the shared tests write and read through the same
// file system. With a separate encryption and decryption key (as happens during key rotation) those
// round trips can never succeed, so only the rotation-specific case below is meaningful here.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestGcsFileSystemWithEncryptionKeyRotation
{
    private final EncryptionKey encryptionKey = randomAes256();
    private final EncryptionKey decryptionKey = randomAes256();

    private GcsFileSystemFactory gcsFileSystemFactory;
    private TrinoFileSystem fileSystem;
    private Storage storage;
    private Location rootLocation;

    @BeforeAll
    void setup()
            throws IOException
    {
        GcsFileSystemConfig config = new GcsFileSystemConfig()
                .setEncryptionKey(encodedKey(encryptionKey))
                .setDecryptionKey(encodedKey(decryptionKey));
        byte[] jsonKeyBytes = Base64.getDecoder().decode(requireEnv("GCP_CREDENTIALS_KEY"));
        GcsServiceAccountAuthConfig authConfig = new GcsServiceAccountAuthConfig().setJsonKey(new String(jsonKeyBytes, UTF_8));
        GcsStorageFactory storageFactory = new GcsStorageFactory(config, new GcsServiceAccountAuth(authConfig));
        this.gcsFileSystemFactory = new GcsFileSystemFactory(config, storageFactory);
        this.storage = storageFactory.create(ConnectorIdentity.ofUser("test"));
        String bucket = RemoteStorageHelper.generateBucketName();
        storage.create(BucketInfo.of(bucket));
        this.rootLocation = Location.of("gs://%s/".formatted(bucket));
        this.fileSystem = gcsFileSystemFactory.create(ConnectorIdentity.ofUser("test"));
    }

    @AfterAll
    void tearDown()
    {
        try {
            Bucket bucket = storage.get(new GcsLocation(rootLocation).bucket());
            for (Blob blob : bucket.list().iterateAll()) {
                storage.delete(blob.getBlobId());
            }
            bucket.delete();
        }
        finally {
            fileSystem = null;
            storage = null;
            rootLocation = null;
            try {
                gcsFileSystemFactory.stop();
            }
            finally {
                gcsFileSystemFactory = null;
            }
        }
    }

    @Test
    void testReadWithConfiguredDecryptionKeyAndWriteWithConfiguredEncryptionKey()
            throws Exception
    {
        Location inputLocation = rootLocation.appendPath("input-with-decryption-key");
        byte[] existingPayload = "existing-payload".getBytes(UTF_8);
        GcsLocation inputGcsLocation = new GcsLocation(inputLocation);
        BlobId inputBlobId = BlobId.of(inputGcsLocation.bucket(), inputGcsLocation.path());
        storage.create(
                BlobInfo.newBuilder(inputBlobId).build(),
                existingPayload,
                Storage.BlobTargetOption.encryptionKey(encodedKey(decryptionKey)));

        byte[] readPayload;
        try (var inputStream = fileSystem.newInputFile(inputLocation).newStream()) {
            readPayload = inputStream.readAllBytes();
        }
        assertThat(readPayload).isEqualTo(existingPayload);

        Location outputLocation = rootLocation.appendPath("output-with-encryption-key");
        byte[] newPayload = "new-payload".getBytes(UTF_8);
        fileSystem.newOutputFile(outputLocation).createOrOverwrite(newPayload);

        GcsLocation outputGcsLocation = new GcsLocation(outputLocation);
        Blob outputBlob = storage.get(
                BlobId.of(outputGcsLocation.bucket(), outputGcsLocation.path()),
                Storage.BlobGetOption.decryptionKey(encodedKey(encryptionKey)));
        assertThat(outputBlob.getContent(Blob.BlobSourceOption.decryptionKey(encodedKey(encryptionKey))))
                .isEqualTo(newPayload);

        assertThatThrownBy(() -> outputBlob.getContent(
                Blob.BlobSourceOption.decryptionKey(encodedKey(decryptionKey))))
                .isInstanceOf(StorageException.class);
    }
}
