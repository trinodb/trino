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
package io.trino.plugin.deltalake.metastore;

import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.TableStatistics;

import java.util.List;
import java.util.Optional;

public interface DeltaLakeMetastore
{
    List<String> getAllDatabases();

    Optional<Database> getDatabase(String databaseName);

    List<String> getAllTables(String databaseName);

    Optional<Table> getTable(String databaseName, String tableName);

    void createDatabase(Database database);

    void dropDatabase(String databaseName, boolean deleteData);

    void createTable(ConnectorSession session, Table table, PrincipalPrivileges principalPrivileges);

    void dropTable(ConnectorSession session, String databaseName, String tableName);

    Optional<MetadataEntry> getMetadata(TableSnapshot tableSnapshot, ConnectorSession session);

    ProtocolEntry getProtocol(ConnectorSession session, TableSnapshot table);

    String getTableLocation(SchemaTableName table, ConnectorSession session);

    TableSnapshot getSnapshot(SchemaTableName table, ConnectorSession session);

    List<AddFileEntry> getValidDataFiles(SchemaTableName table, ConnectorSession session);

    TableStatistics getTableStatistics(ConnectorSession session, DeltaLakeTableHandle tableHandle, Constraint constraint);

    HiveMetastore getHiveMetastore();
}
