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

import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.hive.HideDeltaLakeTables;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.spi.security.ConnectorIdentity;

import javax.inject.Inject;

import java.util.Optional;

public class FileHiveMetastoreFactory
        implements HiveMetastoreFactory
{
    private final FileHiveMetastore metastore;

    @Inject
    public FileHiveMetastoreFactory(NodeVersion nodeVersion, HdfsEnvironment hdfsEnvironment, @HideDeltaLakeTables boolean hideDeltaLakeTables, FileHiveMetastoreConfig config)
    {
        // file metastore does not support impersonation, so just create a single shared instance
        metastore = new FileHiveMetastore(nodeVersion, hdfsEnvironment, hideDeltaLakeTables, config);
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return false;
    }

    @Override
    public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
    {
        return metastore;
    }
}
