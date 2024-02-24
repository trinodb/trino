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
package io.trino.filesystem.ozone;

import io.airlift.log.Logging;
import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.junit.jupiter.api.AfterAll;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestOzoneFileSystem
        extends AbstractTestTrinoFileSystem
{
    private TrinoFileSystem fileSystem;
    private Location rootLocation;
    private OzoneFileSystemFactory fileSystemFactory;
    private ObjectStore objectStore;

    protected void initialize(String gcpCredentialKey)
            throws IOException
    {
        Logging.initialize();
        OzoneFileSystemConfig config = new OzoneFileSystemConfig();
        this.rootLocation = Location.of("o3://%s/%s/".formatted("s3v", "bucket1"));
        fileSystemFactory = new OzoneFileSystemFactory(config);
        fileSystem = fileSystemFactory.create((ConnectorIdentity) null);

        OzoneConfiguration conf = new OzoneConfiguration();
        OzoneClient ozoneClient = OzoneClientFactory.getRpcClient("127.0.1.1", 9862, conf);
        objectStore = ozoneClient.getObjectStore();
    }

    @AfterAll
    final void cleanup()
    {
        fileSystem = null;
        // TODO
//        fileSystemFactory.destroy();
        fileSystemFactory = null;
    }

    // Can't find a way to create exclusively in Ozone createKey api
    // it can be done with createFile() but it use hierarchical fs semantics
    @Override
    protected boolean isCreateExclusive()
    {
        return false;
    }

    @Override
    protected final boolean isHierarchical()
    {
        return false;
    }

    @Override
    protected final TrinoFileSystem getFileSystem()
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
        try {
            OzoneVolume assets = objectStore.getVolume("s3v");
            OzoneBucket bucket1 = assets.getBucket("bucket1");
            assertThat(bucket1.listKeys("/").hasNext()).isFalse();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected final boolean supportsRenameFile()
    {
        return false;
    }

    @AfterAll
    void tearDown()
            throws IOException
    {
        FileIterator fileIterator = fileSystem.listFiles(rootLocation);
        while (fileIterator.hasNext()) {
            FileEntry next = fileIterator.next();
            fileSystem.deleteFile(next.location());
        }
    }

    @Override
    protected Location createLocation(String path)
    {
        if (path.isEmpty()) {
            return getRootLocation();
        }
        // TODO: remove this hack, path should not contains rootLocation
        // Fix this: https://github.com/trinodb/trino/blob/30348401c447691237c57a5f6d6c95eef87560fc/lib/trino-filesystem/src/test/java/io/trino/filesystem/AbstractTestTrinoFileSystem.java#L988
        // eg.
        // Let's say the rootLocation is `/work`
        // foo is a file in `/work/foo`
        // calling createLocation(foo.path()) doesn't make sense
        if (path.startsWith(getRootLocation().path())) {
            return getRootLocation().appendPath(path.substring(getRootLocation().path().length()));
        }

        return getRootLocation().appendPath(path);
    }
}
