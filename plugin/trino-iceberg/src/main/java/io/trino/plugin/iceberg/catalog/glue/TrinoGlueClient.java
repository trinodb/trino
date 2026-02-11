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
package io.trino.plugin.iceberg.catalog.glue;

import io.trino.spi.connector.SchemaTableName;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public interface TrinoGlueClient
{
    Table getTable(SchemaTableName tableName);

    void deleteTable(String databaseName, String tableName);

    void updateTable(String databaseName, TableInput table, Optional<String> versionId);

    void createTable(String databaseName, TableInput table);

    Stream<Table> streamTables(String databaseName);

    Database getDatabase(String databaseName);

    List<String> listDatabases();

    void dropDatabase(String databaseName);

    void createDatabase(DatabaseInput database);
}
