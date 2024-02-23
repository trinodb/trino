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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterAll;

import java.io.IOException;

public abstract class AbstractTestOzoneFileSystem
        extends AbstractTestTrinoFileSystem
{
    private TrinoFileSystem fileSystem;
    private Location rootLocation;
    private OzoneFileSystemFactory fileSystemFactory;

    protected void initialize(String gcpCredentialKey)
            throws IOException
    {
        Logging.initialize();
        OzoneFileSystemConfig config = new OzoneFileSystemConfig();
        this.rootLocation = Location.of("o3://%s/%s/".formatted("s3v", "bucket1"));
        fileSystemFactory = new OzoneFileSystemFactory(config);
        fileSystem = fileSystemFactory.create((ConnectorIdentity) null);
    }

    @AfterAll
    final void cleanup()
    {
        fileSystem = null;
        // TODO
//        fileSystemFactory.destroy();
        fileSystemFactory = null;
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
//        String bucket = new GcsLocation(rootLocation).bucket();
//        assertThat(storage.list(bucket).iterateAll()).isEmpty();
    }

    @Override
    protected final boolean supportsCreateExclusive()
    {
        return true;
    }

    @Override
    protected final boolean supportsRenameFile()
    {
        return false;
    }
}
