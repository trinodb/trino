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

import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractTestGcsFileSystem
        extends AbstractTestTrinoFileSystem
{
    private TrinoFileSystem fileSystem;
    private Location rootLocation;
    private Storage storage;
    private GcsFileSystemFactory gcsFileSystemFactory;

    protected static String getRequiredEnvironmentVariable(String name)
    {
        return requireNonNull(System.getenv(name), "Environment variable not set: " + name);
    }

    protected void initialize(String gcpCredentialKey)
            throws IOException
    {
        // Note: the account needs the following permissions:
        // create/get/delete bucket
        // create/get/list/delete blob
        // For gcp testing this corresponds to the Cluster Storage Admin and Cluster Storage Object Admin roles
        byte[] jsonKeyBytes = Base64.getDecoder().decode(gcpCredentialKey);
        GcsFileSystemConfig config = new GcsFileSystemConfig().setJsonKey(new String(jsonKeyBytes, UTF_8));
        GcsStorageFactory storageFactory = new TestingGcsStorageFactory(config);
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
            throws Exception
    {
        try {
            RemoteStorageHelper.forceDelete(storage, rootLocation.host().get(), 5, TimeUnit.SECONDS);
            gcsFileSystemFactory.stop();
        }
        finally {
            fileSystem = null;
            storage = null;
            rootLocation = null;
            gcsFileSystemFactory = null;
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

    private static class TestingGcsStorageFactory
            implements GcsStorageFactory
    {
        private final Storage storage;

        public TestingGcsStorageFactory(GcsFileSystemConfig config)
        {
            requireNonNull(config, "config is null");
            InputStream inputStream = new ByteArrayInputStream(config.getJsonKey().getBytes(UTF_8));
            // Note: the default project id from the credentials file will be used. See StorageOptions.setProjectId()
            RemoteStorageHelper helper = RemoteStorageHelper.create(null, inputStream);
            this.storage = helper.getOptions().getService();
        }

        @Override
        public Storage create(ConnectorIdentity identity)
        {
            return storage;
        }
    }
}
