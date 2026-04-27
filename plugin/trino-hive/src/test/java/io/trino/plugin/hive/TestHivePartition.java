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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static io.trino.plugin.hive.HiveQueryRunner.HIVE_CATALOG;
import static io.trino.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;

public class TestHivePartition
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setHiveProperties(ImmutableMap.of(
                        "hive.non-managed-table-writes-enabled", "true",
                        "hive.allow-register-partition-procedure", "true"))
                .build();
    }

    @Test
    public void testInsertOverwriteWithCustomPartitionLocation()
            throws Exception
    {
        String tableName = "test_insert_overwrite_custom_partition_" + randomNameSuffix();
        TrinoFileSystem fileSystem = getConnectorService(getQueryRunner(), TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));

        Location tableLocation = Location.of("local:///test_insert_overwrite_" + UUID.randomUUID());
        fileSystem.createDirectory(tableLocation);

        Location customPartitionDir = tableLocation.appendPath("k=k1_plus");
        fileSystem.createDirectory(customPartitionDir);

        try {
            assertUpdate(format(
                    "CREATE TABLE %s.%s.%s (v integer, k varchar) WITH (" +
                            "partitioned_by = ARRAY['k']," +
                            "external_location = '%s')",
                    HIVE_CATALOG, TPCH_SCHEMA, tableName,
                    tableLocation));

            assertUpdate(format(
                    "CALL system.register_partition('%s', '%s', ARRAY['k'], ARRAY['k1'], '%s')",
                    TPCH_SCHEMA, tableName, customPartitionDir.toString()));

            Session appendSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setCatalogSessionProperty("hive", "insert_existing_partitions_behavior", "APPEND")
                    .build();
            assertUpdate(appendSession, format("INSERT INTO %s.%s.%s VALUES (1, 'k1'), (2, 'k2')", HIVE_CATALOG, TPCH_SCHEMA, tableName), 2);
            assertQuery("SELECT v, k FROM " + tableName + " ORDER BY v", "VALUES (1, 'k1'), (2, 'k2')");
            assertQuery(
                    format("SELECT \"$path\" LIKE '%%k=k1_plus%%' FROM %s WHERE k='k1' LIMIT 1", tableName),
                    "VALUES true");
            assertQuery(
                    format("SELECT \"$path\" LIKE '%%k=k2%%' FROM %s WHERE k='k2' LIMIT 1", tableName),
                    "VALUES true");

            Session overwriteSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setCatalogSessionProperty("hive", "insert_existing_partitions_behavior", "OVERWRITE")
                    .build();
            assertUpdate(overwriteSession, format("INSERT INTO %s.%s.%s VALUES (10, 'k1'), (20, 'k2')", HIVE_CATALOG, TPCH_SCHEMA, tableName), 2);

            assertQuery("SELECT v, k FROM " + tableName + " ORDER BY v", "VALUES (10, 'k1'), (20, 'k2')");
            assertQuery(
                    format("SELECT \"$path\" LIKE '%%k=k1_plus%%' FROM %s WHERE k='k1' LIMIT 1", tableName),
                    "VALUES true");
            assertQuery(
                    format("SELECT \"$path\" LIKE '%%k=k2%%' FROM %s WHERE k='k2' LIMIT 1", tableName),
                    "VALUES true");

            assertUpdate("DROP TABLE " + tableName);
        }
        finally {
            fileSystem.deleteDirectory(tableLocation);
        }
    }
}
