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

import io.trino.metastore.Database;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Optional;

public interface DeltaLakeMetastore
{
    List<String> getAllDatabases();

    Optional<Database> getDatabase(String databaseName);

    List<TableInfo> getAllTables(String databaseName);

    Optional<Table> getRawMetastoreTable(String databaseName, String tableName);

    Optional<DeltaMetastoreTable> getTable(String databaseName, String tableName);

    void createDatabase(Database database);

    void dropDatabase(String databaseName, boolean deleteData);

    void createTable(Table table, PrincipalPrivileges principalPrivileges);

    void replaceTable(Table table, PrincipalPrivileges principalPrivileges);

    void dropTable(SchemaTableName schemaTableName, String tableLocation, boolean deleteData);

    void renameTable(SchemaTableName from, SchemaTableName to);
}
