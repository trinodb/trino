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
package io.trino.plugin.iceberg.catalog.file;

import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.iceberg.catalog.AbstractMetastoreTableOperations;
import io.trino.spi.connector.ConnectorSession;
import org.apache.iceberg.io.FileIO;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Optional;

@NotThreadSafe
public class FileMetastoreTableOperations
        extends AbstractMetastoreTableOperations
{
    public FileMetastoreTableOperations(
            FileIO fileIo,
            HiveMetastore metastore,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        super(fileIo, metastore, session, database, table, owner, location);
    }
}
