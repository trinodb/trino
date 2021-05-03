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
package io.trino.plugin.iceberg;

import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.HiveMetastore;
import org.apache.iceberg.TableOperations;

import javax.inject.Inject;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HiveTableOperationsProvider
{
    private final FileIoProvider fileIoProvider;
    private final HiveMetastore hiveMetastore;

    @Inject
    public HiveTableOperationsProvider(FileIoProvider fileIoProvider, HiveMetastore hiveMetastore)
    {
        this.fileIoProvider = requireNonNull(fileIoProvider, "fileIoProvider is null");
        this.hiveMetastore = requireNonNull(hiveMetastore, "hiveMetastore is null");
    }

    public TableOperations createTableOperations(
            HdfsContext hdfsContext,
            String queryId,
            HiveIdentity identity,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        return new HiveTableOperations(
                fileIoProvider.createFileIo(hdfsContext, queryId),
                hiveMetastore,
                identity,
                database,
                table,
                owner,
                location);
    }
}
