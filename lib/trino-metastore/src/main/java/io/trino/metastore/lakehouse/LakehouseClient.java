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
package io.trino.metastore.lakehouse;

import com.google.cloud.biglake.hive.v1.HiveDatabase;
import com.google.cloud.biglake.hive.v1.HiveTable;
import com.google.cloud.biglake.hive.v1.Partition;

import java.util.List;
import java.util.Optional;

/**
 * The reads this metastore makes against a Lakehouse Hive catalog.
 *
 * <p>An object that Lakehouse does not have is an empty result rather than a failure, because the
 * metastore reports a missing object to Trino the same way. Every other failure is a {@link
 * io.trino.spi.TrinoException}.
 */
public interface LakehouseClient
{
    /**
     * Returns the databases of the catalog, or nothing when the catalog does not exist.
     */
    List<HiveDatabase> listDatabases();

    Optional<HiveDatabase> getDatabase(String databaseName);

    /**
     * Returns the tables of the database, or nothing when the database does not exist.
     */
    List<HiveTable> listTables(String databaseName);

    Optional<HiveTable> getTable(String databaseName, String tableName);

    /**
     * Returns the partitions matching {@code filter}, or nothing when the table does not exist.
     *
     * @param filter a Lakehouse partition filter, or an empty string for every partition
     */
    Optional<List<Partition>> listPartitions(String databaseName, String tableName, String filter);
}
