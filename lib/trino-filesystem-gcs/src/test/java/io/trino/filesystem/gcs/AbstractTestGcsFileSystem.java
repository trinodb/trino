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

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.Base64;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkState;
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

    protected void initialize(String gcpCredentialKey, String bucket)
            throws IOException
    {
        // Note: the account needs the following permissions:
        // get bucket
        // create/get/list/delete blob
        // For gcp testing this corresponds to the Cluster Storage Admin and Cluster Storage Object Admin roles
        byte[] jsonKeyBytes = Base64.getDecoder().decode(gcpCredentialKey);
        GcsFileSystemConfig config = new GcsFileSystemConfig().setJsonKey(new String(jsonKeyBytes, UTF_8));
        GcsStorageFactory storageFactory = new DefaultGcsStorageFactory(config);
        this.gcsFileSystemFactory = new GcsFileSystemFactory(config, storageFactory);
        this.storage = storageFactory.create(ConnectorIdentity.ofUser("test"));
        // The bucket must exist.
        checkState(storage.get(bucket) != null, "Bucket not found: '%s'", bucket);
        this.rootLocation = Location.of("gs://%s/%s/".formatted(bucket, UUID.randomUUID()));
        this.fileSystem = gcsFileSystemFactory.create(ConnectorIdentity.ofUser("test"));
        cleanupFiles();
    }

    @AfterAll
    void tearDown()
    {
        try {
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
        GcsLocation gcsLocation = new GcsLocation(rootLocation);
        assertThat(storage.list(gcsLocation.bucket(), BlobListOption.prefix(gcsLocation.path())).iterateAll()).isEmpty();
    }

    @Override
    protected final boolean supportsRenameFile()
    {
        return false;
    }
}
