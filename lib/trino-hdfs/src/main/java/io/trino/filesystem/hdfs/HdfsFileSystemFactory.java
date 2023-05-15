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
package io.trino.filesystem.hdfs;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.spi.security.ConnectorIdentity;

import static java.util.Objects.requireNonNull;

public class HdfsFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final HdfsEnvironment environment;
    private final TrinoHdfsFileSystemStats fileSystemStats;

    @Inject
    public HdfsFileSystemFactory(HdfsEnvironment environment, TrinoHdfsFileSystemStats fileSystemStats)
    {
        this.environment = requireNonNull(environment, "environment is null");
        this.fileSystemStats = requireNonNull(fileSystemStats, "fileSystemStats is null");
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new HdfsFileSystem(environment, new HdfsContext(identity), fileSystemStats);
    }

    public TrinoHdfsFileSystemStats getFileSystemStats()
    {
        return fileSystemStats;
    }
}
