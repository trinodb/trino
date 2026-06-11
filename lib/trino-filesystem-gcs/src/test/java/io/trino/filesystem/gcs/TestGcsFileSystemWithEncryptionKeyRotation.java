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
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import io.trino.filesystem.Location;
import io.trino.filesystem.encryption.EncryptionKey;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;

import static io.trino.filesystem.encryption.EncryptionKey.randomAes256;
import static io.trino.filesystem.gcs.GcsUtils.encodedKey;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestGcsFileSystemWithEncryptionKeyRotation
        extends AbstractTestGcsFileSystem
{
    private final EncryptionKey encryptionKey = randomAes256();
    private final EncryptionKey decryptionKey = randomAes256();

    @BeforeAll
    void setup()
            throws IOException
    {
        initialize(requireEnv("GCP_CREDENTIALS_KEY"), new GcsFileSystemConfig()
                .setEncryptionKey(encodedKey(encryptionKey))
                .setDecryptionKey(encodedKey(decryptionKey)));
    }

    @Test
    void testReadWithConfiguredDecryptionKeyAndWriteWithConfiguredEncryptionKey()
            throws Exception
    {
        Storage storage = getStorage();

        Location inputLocation = getRootLocation().appendPath("input-with-decryption-key");
        byte[] existingPayload = "existing-payload".getBytes(UTF_8);
        GcsLocation inputGcsLocation = new GcsLocation(inputLocation);
        BlobId inputBlobId = BlobId.of(inputGcsLocation.bucket(), inputGcsLocation.path());
        storage.create(
                BlobInfo.newBuilder(inputBlobId).build(),
                existingPayload,
                Storage.BlobTargetOption.encryptionKey(encodedKey(decryptionKey)));

        byte[] readPayload;
        try (var inputStream = getFileSystem().newInputFile(inputLocation).newStream()) {
            readPayload = inputStream.readAllBytes();
        }
        assertThat(readPayload).isEqualTo(existingPayload);

        Location outputLocation = getRootLocation().appendPath("output-with-encryption-key");
        byte[] newPayload = "new-payload".getBytes(UTF_8);
        getFileSystem().newOutputFile(outputLocation).createOrOverwrite(newPayload);

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
