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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.junit.jupiter.api.AfterAll;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;

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
        while(fileIterator.hasNext()) {
            FileEntry next = fileIterator.next();
            fileSystem.deleteFile(next.location());
        }
    }

}
