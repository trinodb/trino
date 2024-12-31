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

package io.trino.plugin.paimon.catalog.file;

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.paimon.PaimonConfig;
import io.trino.plugin.paimon.catalog.AbstractPaimonTrinoCatalog;
import io.trino.spi.connector.ConnectorSession;
import org.apache.paimon.fs.FileIO;

import java.util.List;
import java.util.Optional;

public class TrinoFileSystemPaimonCatalog
        extends AbstractPaimonTrinoCatalog
{
    public TrinoFileSystemPaimonCatalog(
            PaimonConfig config,
            TrinoFileSystemFactory fileSystemFactory)
    {
        super(config, fileSystemFactory);
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String database)
    {
        FileIO fileIO = getFileIO(session);
        return uncheck(() -> fileIO.exists(newDatabasePath(database)));
    }

    @Override
    public List<String> listDatabases(ConnectorSession session)
    {
        return listDatabasesInFileSystem(session);
    }

    @Override
    public List<String> listTables(ConnectorSession session, Optional<String> namespace)
    {
        if (namespace.isPresent()) {
            return listTablesInFileSystem(session, newDatabasePath(namespace.get()));
        }
        else {
            return listDatabases(session).stream()
                    .flatMap(db -> listTablesInFileSystem(session, newDatabasePath(db)).stream())
                    .toList();
        }
    }
}
