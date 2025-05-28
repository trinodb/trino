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
package io.trino.plugin.hive.fs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metastore.Table;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.NoSuchElementException;

import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

// some tests may invalidate the whole cache affecting therefore other concurrent tests
@Execution(SAME_THREAD)
public class TestCachingDirectoryListerRecursiveFilesOnly
        extends BaseCachingDirectoryListerTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunner(ImmutableMap.<String, String>builder()
                .put("hive.allow-register-partition-procedure", "true")
                .put("hive.recursive-directories", "true")
                .put("hive.file-status-cache-expire-time", "5m")
                .put("hive.file-status-cache.max-retained-size", "1MB")
                .put("hive.file-status-cache-tables", "tpch.*")
                .buildOrThrow());
    }

    @Test
    public void testRecursiveDirectories()
    {
        // Create partitioned table to force files to be inserted in sub-partition paths
        assertUpdate("CREATE TABLE recursive_directories (clicks bigint, day date, country varchar) WITH (format = 'ORC', partitioned_by = ARRAY['day', 'country'])");
        assertUpdate("INSERT INTO recursive_directories VALUES (1000, DATE '2022-02-01', 'US'), (2000, DATE '2022-02-01', 'US'), (4000, DATE '2022-02-02', 'US'), (1500, DATE '2022-02-01', 'AT'), (2500, DATE '2022-02-02', 'AT')", 5);

        // Replace the partitioned table a new unpartitioned table with the same root location, leaving the data in place
        Table partitionedTable = getTable(TPCH_SCHEMA, "recursive_directories")
                .orElseThrow(() -> new NoSuchElementException(format("Failed to read table %s.%s", TPCH_SCHEMA, "recursive_directories")));
        // Must not delete the data files when dropping the partitioned table
        dropTable(TPCH_SCHEMA, "recursive_directories", false);
        // Must create the table directly to bypass check that the target directory already exists
        Table testTable = Table.builder(partitionedTable)
                .setPartitionColumns(ImmutableList.of())
                .build();
        createTable(testTable, testTable.getOwner().map(MetastoreUtil::buildInitialPrivilegeSet).orElse(NO_PRIVILEGES));

        // Execute a query on the new table to pull the listing into the cache
        assertQuery("SELECT sum(clicks) FROM recursive_directories", "VALUES (11000)");

        String tableLocation = getTableLocation(TPCH_SCHEMA, "recursive_directories");
        assertThat(isCached(tableLocation, SchemaTableName.schemaTableName(TPCH_SCHEMA, "recursive_directories"))).isTrue();

        // Insert should invalidate cache, even at the root directory path
        assertUpdate("INSERT INTO recursive_directories VALUES (1000)", 1);
        assertThat(isCached(tableLocation, SchemaTableName.schemaTableName(TPCH_SCHEMA, "recursive_directories"))).isFalse();

        // Results should include the new insert which is at the table location root for the unpartitioned table
        assertQuery("SELECT sum(clicks) FROM recursive_directories", "VALUES (12000)");

        assertUpdate("DROP TABLE recursive_directories");

        assertThat(isCached(tableLocation, SchemaTableName.schemaTableName(TPCH_SCHEMA, "recursive_directories"))).isFalse();
    }
}
