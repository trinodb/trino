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

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponseInterceptor;
import com.google.cloud.ServiceOptions;
import com.google.cloud.http.HttpTransportOptions;
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

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.cloud.storage.Storage.BlobTargetOption.doesNotExist;
import static io.trino.filesystem.encryption.EncryptionKey.randomAes256;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractTestGcsFileSystem
        extends AbstractTestTrinoFileSystem
{
    protected final EncryptionKey randomEncryptionKey = randomAes256();
    private final AtomicInteger requests = new AtomicInteger();
    private TrinoFileSystem fileSystem;
    private Location rootLocation;
    private Storage storage;
    private GcsFileSystemFactory gcsFileSystemFactory;

    protected void initialize(String gcpCredentialKey)
            throws IOException
    {
        // Note: the account needs the following permissions:
        // create/get/delete bucket
        // create/get/list/delete blob
        // For gcp testing this corresponds to the Cluster Storage Admin and Cluster Storage Object Admin roles
        byte[] jsonKeyBytes = Base64.getDecoder().decode(gcpCredentialKey);
        GcsServiceAccountAuthConfig authConfig = new GcsServiceAccountAuthConfig().setJsonKey(new String(jsonKeyBytes, UTF_8));
        initialize(new GcsFileSystemConfig(), new GcsServiceAccountAuth(authConfig));
    }

    protected void initialize(GcsFileSystemConfig config, GcsAuth gcsAuth)
            throws IOException
    {
        // GcsAuth is the only hook into the storage options, so it also installs the request counter
        GcsAuth countingAuth = (builder, identity) -> {
            gcsAuth.setAuth(builder, identity);
            builder.setTransportOptions(new RequestCountingTransportOptions(requests));
        };
        GcsStorageFactory storageFactory = new GcsStorageFactory(config, countingAuth);
        this.gcsFileSystemFactory = new GcsFileSystemFactory(config, storageFactory);
        this.storage = storageFactory.create(ConnectorIdentity.ofUser("test"));
        String bucket = RemoteStorageHelper.generateBucketName();
        storage.create(BucketInfo.of(bucket));
        this.rootLocation = Location.of("gs://%s/".formatted(bucket));
        this.fileSystem = gcsFileSystemFactory.create(ConnectorIdentity.ofUser("test"));
        cleanupFiles();
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
        assertThat(storage.list(bucket).iterateAll()).isEmpty();
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
        BlobId blobId = BlobId.of(new GcsLocation(rootLocation).bucket(), "data/file/");
        storage.create(BlobInfo.newBuilder(blobId).build(), new byte[0], doesNotExist());
        try {
            assertThat(fileSystem.listFiles(getRootLocation()).hasNext()).isFalse();

            Location data = getRootLocation().appendPath("data/");
            assertThat(fileSystem.listDirectories(getRootLocation())).containsExactly(data);
            assertThat(fileSystem.listDirectories(data)).containsExactly(data.appendPath("file/"));

            // blobs ending in slash are deleted, even though they are not visible to listings
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
        // According to https://docs.cloud.google.com/storage/docs/objects#recommendations some chars ([*]#?) are discouraged
        // because they are specially treated in gcloud cli. But they are not directly prohibited.
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

    @Test
    void testInputFileExistsSendsOneRequest()
            throws IOException
    {
        Location location = getRootLocation().appendPath("exists");
        TrinoInputFile inputFile = getFileSystem().newInputFile(location);

        requests.set(0);
        assertThat(inputFile.exists()).isFalse();
        assertThat(requests.get()).isEqualTo(1);

        getFileSystem().newOutputFile(location).createOrOverwrite(new byte[] {1});
        requests.set(0);
        assertThat(inputFile.exists()).isTrue();
        assertThat(requests.get()).isEqualTo(1);
    }

    @Test
    void testKnownLengthReadsSendOneRequest()
            throws IOException
    {
        Location location = getRootLocation().appendPath("known-length");
        getFileSystem().newOutputFile(location).createOrOverwrite("0123456789".getBytes(UTF_8));
        byte[] buffer = new byte[4];

        try (TrinoInput input = getFileSystem().newInputFile(location, 10).newInput()) {
            requests.set(0);
            input.readFully(2, buffer, 0, 4);
            assertThat(new String(buffer, UTF_8)).isEqualTo("2345");
            assertThat(requests.get()).isEqualTo(1);

            requests.set(0);
            assertThat(input.readTail(buffer, 0, 4)).isEqualTo(4);
            assertThat(new String(buffer, UTF_8)).isEqualTo("6789");
            assertThat(requests.get()).isEqualTo(1);

            assertThatThrownBy(() -> input.readFully(9, buffer, 0, 2))
                    .isInstanceOf(EOFException.class);
        }

        try (TrinoInput input = getFileSystem().newInputFile(location).newInput()) {
            requests.set(0);
            input.readFully(2, buffer, 0, 4);
            assertThat(requests.get()).isEqualTo(2);
        }

        try (TrinoInput input = getFileSystem().newInputFile(location, 10).newInput()) {
            getFileSystem().deleteFile(location);
            assertThatThrownBy(() -> input.readFully(0, buffer, 0, 1))
                    .isInstanceOf(FileNotFoundException.class);
            assertThatThrownBy(() -> input.readTail(buffer, 0, 1))
                    .isInstanceOf(FileNotFoundException.class);
            assertThatThrownBy(() -> input.readFully(10, buffer, 0, 1))
                    .isInstanceOf(FileNotFoundException.class);
        }

        try (TrinoInput input = getFileSystem().newInputFile(location, 0).newInput()) {
            assertThatThrownBy(() -> input.readTail(buffer, 0, 1))
                    .isInstanceOf(FileNotFoundException.class);
        }
    }

    private static final class RequestCountingTransportOptions
            extends HttpTransportOptions
    {
        private final AtomicInteger requests;

        private RequestCountingTransportOptions(AtomicInteger requests)
        {
            super(HttpTransportOptions.newBuilder());
            this.requests = requests;
        }

        @Override
        public HttpRequestInitializer getHttpRequestInitializer(ServiceOptions<?, ?> serviceOptions)
        {
            HttpRequestInitializer initializer = super.getHttpRequestInitializer(serviceOptions);
            return request -> {
                initializer.initialize(request);
                HttpResponseInterceptor interceptor = request.getResponseInterceptor();
                request.setResponseInterceptor(response -> {
                    requests.incrementAndGet();
                    if (interceptor != null) {
                        interceptor.interceptResponse(response);
                    }
                });
            };
        }
    }
}
