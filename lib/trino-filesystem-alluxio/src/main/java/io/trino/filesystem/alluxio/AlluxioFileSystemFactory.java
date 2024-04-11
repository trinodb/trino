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
package io.trino.filesystem.alluxio;

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;

public class AlluxioFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final AlluxioConfiguration conf;

    @Inject
    public AlluxioFileSystemFactory()
    {
        this(Configuration.global());
    }

    public AlluxioFileSystemFactory(AlluxioConfiguration conf)
    {
        this.conf = conf;
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        FileSystemContext fsContext = FileSystemContext.create(conf);
        FileSystem alluxioClient = FileSystem.Factory.create(fsContext);
        return new AlluxioFileSystem(alluxioClient);
    }
}
