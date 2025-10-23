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
package io.trino.plugin.hive.metastore.file;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.spi.NodeVersion;

import java.io.File;

import static com.google.common.base.Verify.verify;

public final class TestingFileHiveMetastore
{
    private TestingFileHiveMetastore() {}

    public static FileHiveMetastore createTestingFileHiveMetastore(File catalogDirectory)
    {
        verify(catalogDirectory.mkdirs());
        return createTestingFileHiveMetastore(
                new LocalFileSystemFactory(catalogDirectory.toPath()),
                Location.of("local:///"));
    }

    public static FileHiveMetastore createTestingFileHiveMetastore(TrinoFileSystemFactory fileSystemFactory, Location catalogDirectory)
    {
        return new FileHiveMetastore(
                new NodeVersion("testversion"),
                fileSystemFactory,
                new HiveMetastoreConfig().isHideDeltaLakeTables(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(catalogDirectory.toString())
                        .setMetastoreUser("test"));
    }
}
