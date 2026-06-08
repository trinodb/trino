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
import com.google.cloud.storage.testing.RemoteStorageHelper;
import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.encryption.EncryptionEnforcingFileSystem;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.Base64;
import java.util.UUID;
import java.util.function.Consumer;

import static com.google.cloud.storage.Storage.BlobTargetOption.doesNotExist;
import static io.trino.filesystem.encryption.EncryptionKey.randomAes256;
import static io.trino.filesystem.gcs.GcsFileSystemConfig.AuthType.APPLICATION_DEFAULT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractTestGcsFileSystem
        extends AbstractTestTrinoFileSystem
{
    protected final EncryptionKey randomEncryptionKey = randomAes256();
    private TrinoFileSystem fileSystem;
    private Location rootLocation;
    private Storage storage;
    private GcsFileSystemFactory gcsFileSystemFactory;
    private boolean ownsBucket;

    protected void initialize(String gcpCredentialKey)
            throws IOException
    {
        initialize(gcpCredentialKey, null);
    }

    protected void initialize(String gcpCredentialKey, String staticBucket)
            throws IOException
    {
        initialize(gcpCredentialKey, staticBucket, _ -> {});
    }

    protected void initialize(String gcpCredentialKey, String staticBucket, Consumer<GcsFileSystemConfig> configCustomizer)
            throws IOException
    {
        // Note: the account needs the following permissions:
        // create/get/delete bucket
        // create/get/list/delete blob
        // For gcp testing this corresponds to the Cluster Storage Admin and Cluster
        // Storage Object
        // Admin roles
        byte[] jsonKeyBytes = Base64.getDecoder().decode(gcpCredentialKey);
        GcsFileSystemConfig config = new GcsFileSystemConfig();
        configCustomizer.accept(config);
        GcsServiceAccountAuthConfig authConfig = new GcsServiceAccountAuthConfig()
                .setJsonKey(new String(jsonKeyBytes, UTF_8));
        GcsStorageFactory storageFactory = new GcsStorageFactory(config, new GcsServiceAccountAuth(authConfig));
        AnalyticsCoreGcsFileSystemFactory analyticsCoreGcsFileSystemFactory = new AnalyticsCoreGcsFileSystemFactory(config);
        initializeStorage(config, storageFactory, analyticsCoreGcsFileSystemFactory, staticBucket);
    }

    protected void initializeWithApplicationDefault(String staticBucket, Consumer<GcsFileSystemConfig> configCustomizer)
            throws IOException
    {
        GcsFileSystemConfig config = new GcsFileSystemConfig()
                .setAuthType(APPLICATION_DEFAULT);
        configCustomizer.accept(config);
        GcsStorageFactory storageFactory = new GcsStorageFactory(config, new ApplicationDefaultAuth());
        AnalyticsCoreGcsFileSystemFactory analyticsCoreGcsFileSystemFactory = new AnalyticsCoreGcsFileSystemFactory(config);
        initializeStorage(config, storageFactory, analyticsCoreGcsFileSystemFactory, staticBucket);
    }

    private void initializeStorage(
            GcsFileSystemConfig config,
            GcsStorageFactory storageFactory,
            AnalyticsCoreGcsFileSystemFactory analyticsCoreGcsFileSystemFactory,
            String staticBucket)
            throws IOException
    {
        this.gcsFileSystemFactory = new GcsFileSystemFactory(
                config,
                storageFactory,
                analyticsCoreGcsFileSystemFactory);
        this.storage = storageFactory.create(ConnectorIdentity.ofUser("test"));
        if (staticBucket != null && !staticBucket.isEmpty()) {
            String uniquePath = UUID.randomUUID().toString();
            this.rootLocation = Location.of("gs://%s/%s/".formatted(staticBucket, uniquePath));
            this.ownsBucket = false;
        }
        else {
            String bucket = RemoteStorageHelper.generateBucketName();
            storage.create(BucketInfo.of(bucket));
            this.rootLocation = Location.of("gs://%s/".formatted(bucket));
            this.ownsBucket = true;
        }
        this.fileSystem = gcsFileSystemFactory.create(ConnectorIdentity.ofUser("test"));
        cleanupFiles();
    }

    @AfterAll
    void tearDown()
    {
        try {
            String bucketName = new GcsLocation(rootLocation).bucket();
            if (ownsBucket) {
                Bucket bucket = storage.get(bucketName);
                for (Blob blob : bucket.list().iterateAll()) {
                    storage.delete(blob.getBlobId());
                }
                bucket.delete();
            }
            else {
                String prefix = new GcsLocation(rootLocation).path();
                for (Blob blob : storage.list(bucketName, Storage.BlobListOption.prefix(prefix)).iterateAll()) {
                    storage.delete(blob.getBlobId());
                }
            }
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

    @AfterEach
    void afterEach()
            throws IOException
    {
        cleanupFiles();
    }

    private void cleanupFiles()
            throws IOException
    {
        fileSystem.deleteDirectory(getRootLocation());
    }

    @Override
    protected boolean isHierarchical()
    {
        return false;
    }

    @Override
    protected TrinoFileSystem getFileSystem()
    {
        if (useServerSideEncryptionWithCustomerKey()) {
            return new EncryptionEnforcingFileSystem(fileSystem, randomEncryptionKey);
        }
        return fileSystem;
    }

    @Override
    protected Location getRootLocation()
    {
        return rootLocation;
    }

    @Override
    protected void verifyFileSystemIsEmpty()
    {
        String bucket = new GcsLocation(rootLocation).bucket();
        String prefix = new GcsLocation(rootLocation).path();
        if (prefix.isEmpty()) {
            assertThat(storage.list(bucket).iterateAll()).isEmpty();
        }
        else {
            assertThat(storage.list(bucket, Storage.BlobListOption.prefix(prefix)).iterateAll())
                    .isEmpty();
        }
    }

    @Override
    protected final boolean supportsRenameFile()
    {
        return false;
    }

    @Override
    protected boolean supportsPreSignedUri()
    {
        return true;
    }

    @Test
    void testExistingFileWithTrailingSlash()
            throws IOException
    {
        GcsLocation gcsRootLocation = new GcsLocation(rootLocation);
        BlobId blobId = BlobId.of(gcsRootLocation.bucket(), gcsRootLocation.path() + "data/file/");
        storage.create(BlobInfo.newBuilder(blobId).build(), new byte[0], doesNotExist());
        try {
            assertThat(fileSystem.listFiles(getRootLocation()).hasNext()).isFalse();

            Location data = getRootLocation().appendPath("data/");
            assertThat(fileSystem.listDirectories(getRootLocation())).containsExactly(data);
            assertThat(fileSystem.listDirectories(data)).containsExactly(data.appendPath("file/"));

            // blobs ending in slash are deleted, even though they are not visible to
            // listings
            fileSystem.deleteDirectory(data);
            assertThat(fileSystem.listDirectories(getRootLocation())).isEmpty();
        }
        finally {
            storage.delete(blobId);
        }
    }

    @Test
    void testRoundTripFileWithDiscouragedCharsName()
            throws Exception
    {
        // According to
        // https://docs.cloud.google.com/storage/docs/objects#recommendations some chars
        // ([*]#?) are discouraged
        // because they are specially treated in gcloud cli. But they are not directly
        // prohibited.
        byte[] buffer = new byte[8];
        String stringToWrite = "test";
        Location fileLocation = getRootLocation().appendPath("[*]#?");
        TrinoOutputFile outputFile = getFileSystem().newOutputFile(fileLocation);
        outputFile.createOrOverwrite(stringToWrite.getBytes(UTF_8));
        TrinoInputFile inputFile = getFileSystem().newInputFile(fileLocation);
        try (TrinoInput trinoInput = inputFile.newInput()) {
            int readBytes = trinoInput.readTail(buffer, 0, 8);
            String readString = new String(buffer, 0, readBytes, UTF_8);
            assertThat(readString).isEqualTo(stringToWrite);
        }
    }
}
