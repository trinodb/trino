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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.security.PrincipalType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestInMemoryGlueCache
{
    private static GlueCache createGlueCache()
    {
        return new InMemoryGlueCache(
                new CatalogName("testing"),
                new Duration(1, TimeUnit.DAYS),
                new Duration(1, TimeUnit.DAYS),
                Optional.of(new Duration(12, TimeUnit.HOURS)),
                1,
                Long.MAX_VALUE);
    }

    @Test
    void testGetDatabaseNames()
    {
        GlueCache glueCache = createGlueCache();
        assertThat(glueCache.getDatabaseNames(_ -> ImmutableList.of("db1", "db2"))).containsExactly("db1", "db2");
        // call back is not invoked when the names are already in the cache
        assertThat(glueCache.getDatabaseNames(_ -> ImmutableList.of("fail"))).containsExactly("db1", "db2");
        assertThat(glueCache.getDatabaseNames(_ -> { throw new RuntimeException(); })).containsExactly("db1", "db2");

        glueCache.invalidateDatabaseNames();
        assertThat(glueCache.getDatabaseNames(_ -> ImmutableList.of("db5", "db6"))).containsExactly("db5", "db6");

        glueCache.invalidateDatabaseNames();
        assertThatThrownBy(() -> glueCache.getDatabaseNames(_ -> { throw new TrinoException(HIVE_METASTORE_ERROR, "test"); }))
                .isInstanceOf(TrinoException.class)
                .hasMessage("test");
        assertThat(glueCache.getDatabaseNames(_ -> ImmutableList.of("after exception"))).containsExactly("after exception");
    }

    @Test
    void testGetDatabaseNamesWithCacheDatabase()
            throws InterruptedException
    {
        GlueCache glueCache = createGlueCache();
        assertThat(glueCache.getDatabaseNames(
                cacheDatabase -> {
                    cacheDatabase.accept(testDatabase("db1", "initial"));
                    cacheDatabase.accept(testDatabase("db2", "initial"));
                    return ImmutableList.of("db1", "db2");
                }))
                .containsExactly("db1", "db2");

        // db1 and db2 are already in the cache
        assertThat(glueCache.getDatabase("db1", () -> { throw new RuntimeException(); })).contains(testDatabase("db1", "initial"));
        assertThat(glueCache.getDatabase("db2", () -> { throw new RuntimeException(); })).contains(testDatabase("db2", "initial"));

        glueCache.invalidateDatabaseNames();

        // db1 and db2 are still in the cache after invalidating the database names
        assertThat(glueCache.getDatabase("db1", () -> { throw new RuntimeException(); })).contains(testDatabase("db1", "initial"));
        assertThat(glueCache.getDatabase("db2", () -> { throw new RuntimeException(); })).contains(testDatabase("db2", "initial"));

        // callback can override the current cached values
        assertThat(glueCache.getDatabaseNames(
                cacheDatabase -> {
                    cacheDatabase.accept(testDatabase("db2", "updated"));
                    cacheDatabase.accept(testDatabase("db3", "initial"));
                    cacheDatabase.accept(testDatabase("db4", "initial"));
                    // in a normal system db1 and db4 should be returned, but there is no enforcement of this in the cache
                    return ImmutableList.of("db2", "db3");
                }))
                .containsExactly("db2", "db3");

        assertThat(glueCache.getDatabase("db1", () -> { throw new RuntimeException(); })).contains(testDatabase("db1", "initial"));
        assertThat(glueCache.getDatabase("db2", () -> { throw new RuntimeException(); })).contains(testDatabase("db2", "updated"));
        assertThat(glueCache.getDatabase("db3", () -> { throw new RuntimeException(); })).contains(testDatabase("db3", "initial"));
        assertThat(glueCache.getDatabase("db4", () -> { throw new RuntimeException(); })).contains(testDatabase("db4", "initial"));

        // database invalidation does not invalidate the database names
        glueCache.invalidateDatabase("unknown");
        assertThat(glueCache.getDatabaseNames(cacheDatabase -> { throw new RuntimeException(); })).containsExactly("db2", "db3");

        glueCache.invalidateDatabaseNames();

        // db 1-4 should still be cached
        assertThat(glueCache.getDatabase("db1", () -> { throw new RuntimeException(); })).contains(testDatabase("db1", "initial"));
        assertThat(glueCache.getDatabase("db2", () -> { throw new RuntimeException(); })).contains(testDatabase("db2", "updated"));
        assertThat(glueCache.getDatabase("db3", () -> { throw new RuntimeException(); })).contains(testDatabase("db3", "initial"));
        assertThat(glueCache.getDatabase("db4", () -> { throw new RuntimeException(); })).contains(testDatabase("db4", "initial"));

        // any database invalidation during a bulk load immediately prevents blocks direct caching of full objects
        CountDownLatch inBulkLoad = new CountDownLatch(1);
        CountDownLatch invalidated = new CountDownLatch(1);

        Thread thread = Thread.startVirtualThread(() -> assertThat(glueCache.getDatabaseNames(
                cacheDatabase -> {
                    cacheDatabase.accept(testDatabase("db1", "bulk"));
                    cacheDatabase.accept(testDatabase("db2", "bulk"));
                    inBulkLoad.countDown();
                    awaitUninterruptibly(invalidated);
                    cacheDatabase.accept(testDatabase("db3", "bulk"));
                    cacheDatabase.accept(testDatabase("db4", "bulk"));
                    return ImmutableList.of("db1", "db2", "db3", "db4");
                }))
                .containsExactly("db1", "db2", "db3", "db4"));

        awaitUninterruptibly(inBulkLoad);
        glueCache.invalidateDatabase("unknown");
        invalidated.countDown();
        thread.join();

        // db1 and db2 are now cached with the updated value from the bulk load
        assertThat(glueCache.getDatabase("db1", () -> { throw new RuntimeException(); })).contains(testDatabase("db1", "bulk"));
        assertThat(glueCache.getDatabase("db2", () -> { throw new RuntimeException(); })).contains(testDatabase("db2", "bulk"));
        // db3 and db4 are not updated since an invalidation occurred during the bulk load
        assertThat(glueCache.getDatabase("db3", () -> { throw new RuntimeException(); })).contains(testDatabase("db3", "initial"));
        assertThat(glueCache.getDatabase("db4", () -> { throw new RuntimeException(); })).contains(testDatabase("db4", "initial"));
    }

    @Test
    void testGetDatabase()
    {
        GlueCache glueCache = createGlueCache();
        assertThat(glueCache.getDatabase("db1", () -> Optional.of(testDatabase("db1")))).contains(testDatabase("db1"));
        assertThat(glueCache.getDatabase("db1", () -> Optional.of(testDatabase("fail")))).contains(testDatabase("db1"));
        assertThat(glueCache.getDatabase("db1", () -> { throw new RuntimeException(); })).contains(testDatabase("db1"));

        glueCache.invalidateDatabase("db1");
        assertThat(glueCache.getDatabase("db1", () -> Optional.of(testDatabase("db1", "alternate")))).contains(testDatabase("db1", "alternate"));

        glueCache.invalidateDatabase("db1");
        assertThatThrownBy(() -> glueCache.getDatabase("db1", () -> { throw new TrinoException(HIVE_METASTORE_ERROR, "test"); }))
                .isInstanceOf(TrinoException.class)
                .hasMessage("test");
        assertThat(glueCache.getDatabase("db1", () -> Optional.of(testDatabase("after exception")))).contains(testDatabase("after exception"));
    }

    @Test
    void testGetTables()
    {
        GlueCache glueCache = createGlueCache();
        assertThat(glueCache.getTables("db1", cacheTable -> ImmutableList.of(testTableInfo("db1", "table1"), testTableInfo("db1", "table2"))))
                .containsExactlyInAnyOrder(testTableInfo("db1", "table1"), testTableInfo("db1", "table2"));
        assertThat(glueCache.getTables("db1", cacheTable -> ImmutableList.of(testTableInfo("db1", "fail"))))
                .containsExactlyInAnyOrder(testTableInfo("db1", "table1"), testTableInfo("db1", "table2"));
        assertThat(glueCache.getTables("db1", cacheTable -> { throw new RuntimeException(); }))
                .containsExactlyInAnyOrder(testTableInfo("db1", "table1"), testTableInfo("db1", "table2"));

        glueCache.invalidateTables("db1");
        assertThat(glueCache.getTables("db1", cacheTable -> ImmutableList.of(testTableInfo("db1", "table3"), testTableInfo("db1", "table4"))))
                .containsExactlyInAnyOrder(testTableInfo("db1", "table3"), testTableInfo("db1", "table4"));

        glueCache.invalidateTables("db1");
        assertThatThrownBy(() -> glueCache.getTables("db1", cacheTable -> { throw new TrinoException(HIVE_METASTORE_ERROR, "test"); }))
                .isInstanceOf(TrinoException.class)
                .hasMessage("test");
        assertThat(glueCache.getTables("db1", cacheTable -> ImmutableList.of(testTableInfo("db1", "after exception"))))
                .containsExactlyInAnyOrder(testTableInfo("db1", "after exception"));
    }

    @Test
    void testGetTablesWithCacheDatabase()
            throws InterruptedException
    {
        GlueCache glueCache = createGlueCache();
        assertThat(glueCache.getTables(
                "db1",
                cacheTable -> {
                    cacheTable.accept(testTable("db1", "table1", "initial"));
                    cacheTable.accept(testTable("db1", "table2", "initial"));
                    return ImmutableList.of(testTableInfo("db1", "table1"), testTableInfo("db1", "table2"));
                }))
                .containsExactlyInAnyOrder(testTableInfo("db1", "table1"), testTableInfo("db1", "table2"));

        // table1 and table2 are already in the cache
        assertThat(glueCache.getTable("db1", "table1", () -> { throw new RuntimeException(); })).contains(testTable("db1", "table1", "initial"));
        assertThat(glueCache.getTable("db1", "table2", () -> { throw new RuntimeException(); })).contains(testTable("db1", "table2", "initial"));

        glueCache.invalidateTables("db1");

        // table1 and table2 are still in the cache after invalidating the tables
        assertThat(glueCache.getTable("db1", "table1", () -> { throw new RuntimeException(); })).contains(testTable("db1", "table1", "initial"));
        assertThat(glueCache.getTable("db1", "table2", () -> { throw new RuntimeException(); })).contains(testTable("db1", "table2", "initial"));

        // callback can override the current cached values
        assertThat(glueCache.getTables(
                "db1",
                cacheTable -> {
                    cacheTable.accept(testTable("db1", "table2", "updated"));
                    cacheTable.accept(testTable("db1", "table3", "initial"));
                    cacheTable.accept(testTable("db1", "table4", "initial"));
                    // in a normal system table1 and table4 should be returned, but there is no enforcement of this in the cache
                    return ImmutableList.of(testTableInfo("db1", "table2"), testTableInfo("db1", "table3"));
                }))
                .containsExactlyInAnyOrder(testTableInfo("db1", "table2"), testTableInfo("db1", "table3"));

        assertThat(glueCache.getTable("db1", "table1", () -> { throw new RuntimeException(); })).contains(testTable("db1", "table1", "initial"));
        assertThat(glueCache.getTable("db1", "table2", () -> { throw new RuntimeException(); })).contains(testTable("db1", "table2", "updated"));
        assertThat(glueCache.getTable("db1", "table3", () -> { throw new RuntimeException(); })).contains(testTable("db1", "table3", "initial"));
        assertThat(glueCache.getTable("db1", "table4", () -> { throw new RuntimeException(); })).contains(testTable("db1", "table4", "initial"));

        // table invalidation does not invalidate the database tables
        glueCache.invalidateTable("db1", "unknown", true);
        assertThat(glueCache.getTables("db1", cacheTable -> { throw new RuntimeException(); })).containsExactlyInAnyOrder(testTableInfo("db1", "table2"), testTableInfo("db1", "table3"));

        glueCache.invalidateTables("db1");

        // table 1-4 should still be cached
        assertThat(glueCache.getTable("db1", "table1", () -> { throw new RuntimeException(); })).contains(testTable("db1", "table1", "initial"));
        assertThat(glueCache.getTable("db1", "table2", () -> { throw new RuntimeException(); })).contains(testTable("db1", "table2", "updated"));
        assertThat(glueCache.getTable("db1", "table3", () -> { throw new RuntimeException(); })).contains(testTable("db1", "table3", "initial"));

        // any database invalidation during a bulk load immediately prevents blocks direct caching of full objects
        CountDownLatch inBulkLoad = new CountDownLatch(1);
        CountDownLatch invalidated = new CountDownLatch(1);

        Thread thread = Thread.startVirtualThread(() -> assertThat(glueCache.getTables(
                "db1",
                cacheTable -> {
                    cacheTable.accept(testTable("db1", "table1", "bulk"));
                    cacheTable.accept(testTable("db1", "table2", "bulk"));
                    inBulkLoad.countDown();
                    awaitUninterruptibly(invalidated);
                    cacheTable.accept(testTable("db1", "table3", "bulk"));
                    cacheTable.accept(testTable("db1", "table4", "bulk"));
                    return ImmutableList.of(testTableInfo("db1", "table1"), testTableInfo("db1", "table2"), testTableInfo("db1", "table3"), testTableInfo("db1", "table4"));
                }))
                .containsExactlyInAnyOrder(testTableInfo("db1", "table1"), testTableInfo("db1", "table2"), testTableInfo("db1", "table3"), testTableInfo("db1", "table4")));

        awaitUninterruptibly(inBulkLoad);
        glueCache.invalidateTable("db1", "unknown", true);
        invalidated.countDown();
        thread.join();

        // table1 and table2 are now cached with the updated value from the bulk load
        assertThat(glueCache.getTable("db1", "table1", () -> { throw new RuntimeException(); })).contains(testTable("db1", "table1", "bulk"));
        assertThat(glueCache.getTable("db1", "table2", () -> { throw new RuntimeException(); })).contains(testTable("db1", "table2", "bulk"));
        // table3 and table4 are not updated since an invalidation occurred during the bulk load
        assertThat(glueCache.getTable("db1", "table3", () -> { throw new RuntimeException(); })).contains(testTable("db1", "table3", "initial"));
        assertThat(glueCache.getTable("db1", "table4", () -> { throw new RuntimeException(); })).contains(testTable("db1", "table4", "initial"));
    }

    @Test
    void testGetTable()
    {
        GlueCache glueCache = createGlueCache();
        assertThat(glueCache.getTable("db1", "table1", () -> Optional.of(testTable("db1", "table1"))))
                .contains(testTable("db1", "table1"));
        assertThat(glueCache.getTable("db1", "table1", () -> Optional.of(testTable("db1", "fail"))))
                .contains(testTable("db1", "table1"));
        assertThat(glueCache.getTable("db1", "table1", () -> { throw new RuntimeException(); }))
                .contains(testTable("db1", "table1"));

        glueCache.invalidateTable("db1", "table1", true);
        assertThat(glueCache.getTable("db1", "table1", () -> Optional.of(testTable("db1", "what"))))
                .contains(testTable("db1", "what"));

        glueCache.invalidateTable("db1", "table1", true);
        assertThatThrownBy(() -> glueCache.getTable("db1", "table1", () -> { throw new TrinoException(HIVE_METASTORE_ERROR, "test"); }))
                .isInstanceOf(TrinoException.class)
                .hasMessage("test");
        assertThat(glueCache.getTable("db1", "table1", () -> Optional.of(testTable("db1", "after exception"))))
                .contains(testTable("db1", "after exception"));
    }

    @Test
    void testGetTableColumnStatistics()
    {
        GlueCache glueCache = createGlueCache();

        // column stats missing in the backend system, and are simply missing from the result map
        assertThat(glueCache.getTableColumnStatistics("db", "table", Set.of("col1", "col2"),
                missingColumns -> {
                    assertThat(missingColumns).containsExactlyInAnyOrder("col1", "col2");
                    return Map.of();
                }))
                .isEmpty();

        // stats that fail to load are cached, callback is not invoked
        assertThat(glueCache.getTableColumnStatistics("db", "table", Set.of("col1", "col2"), missingColumns -> { throw new RuntimeException(); }))
                .isEmpty();

        glueCache.invalidateTableColumnStatistics("db", "table");
        assertThat(glueCache.getTableColumnStatistics("db", "table", Set.of("col1", "col2"),
                missingColumns -> {
                    assertThat(missingColumns).containsExactlyInAnyOrder("col1", "col2");
                    return Map.of("col1", testStats(1), "col2", testStats(2));
                }))
                .isEqualTo(Map.of("col1", testStats(1), "col2", testStats(2)));

        assertThat(glueCache.getTableColumnStatistics("db", "table", Set.of("col1", "col2"), missingColumns -> { throw new RuntimeException(); }))
                .containsExactlyInAnyOrderEntriesOf(Map.of("col1", testStats(1), "col2", testStats(2)));

        // additional columns can be requested and only missing columns are fetched
        assertThat(glueCache.getTableColumnStatistics("db", "table", Set.of("col1", "col2", "col3", "col4"),
                missingColumns -> {
                    assertThat(missingColumns).containsExactlyInAnyOrder("col3", "col4");
                    return Map.of("col3", testStats(3), "col4", testStats(4));
                }))
                .isEqualTo(Map.of("col1", testStats(1), "col2", testStats(2), "col3", testStats(3), "col4", testStats(4)));
        assertThat(glueCache.getTableColumnStatistics("db", "table", Set.of("col1", "col2", "col3", "col4"), missingColumns -> { throw new RuntimeException(); }))
                .isEqualTo(Map.of("col1", testStats(1), "col2", testStats(2), "col3", testStats(3), "col4", testStats(4)));

        // table invalidation invalidates the column stats (cascade is not required)
        glueCache.invalidateTable("db", "table", false);
        assertThat(glueCache.getTableColumnStatistics("db", "table", Set.of("col1", "col2"),
                missingColumns -> {
                    assertThat(missingColumns).containsExactlyInAnyOrder("col1", "col2");
                    return Map.of("col1", testStats(11), "col2", testStats(22));
                }))
                .isEqualTo(Map.of("col1", testStats(11), "col2", testStats(22)));

        // database invalidation invalidates the column stats
        glueCache.invalidateDatabase("db");
        assertThat(glueCache.getTableColumnStatistics("db", "table", Set.of("col1", "col2"),
                missingColumns -> {
                    assertThat(missingColumns).containsExactlyInAnyOrder("col1", "col2");
                    return Map.of("col1", testStats(111), "col2", testStats(222));
                }))
                .isEqualTo(Map.of("col1", testStats(111), "col2", testStats(222)));
    }

    @Test
    void testGetPartitionNames()
    {
        GlueCache glueCache = createGlueCache();
        assertThat(glueCache.getPartitionNames("db1", "table1", "", _ -> Set.of(testPartitionName("part1"), testPartitionName("part2"))))
                .containsExactlyInAnyOrder(testPartitionName("part1"), testPartitionName("part2"));
        assertThat(glueCache.getPartitionNames("db1", "table1", "", _ -> Set.of(testPartitionName("fail"))))
                .containsExactlyInAnyOrder(testPartitionName("part1"), testPartitionName("part2"));
        assertThat(glueCache.getPartitionNames("db1", "table1", "", _ -> { throw new RuntimeException(); }))
                .containsExactlyInAnyOrder(testPartitionName("part1"), testPartitionName("part2"));

        // each expression has a different cache
        assertThat(glueCache.getPartitionNames("db1", "table1", "expression", _ -> Set.of(testPartitionName("a"), testPartitionName("b"))))
                .containsExactlyInAnyOrder(testPartitionName("a"), testPartitionName("b"));
        assertThat(glueCache.getPartitionNames("db1", "table1", "expression", _ -> { throw new RuntimeException(); }))
                .containsExactlyInAnyOrder(testPartitionName("a"), testPartitionName("b"));

        // invalidation invalidates all expressions
        glueCache.invalidateTable("db1", "table1", true);
        assertThat(glueCache.getPartitionNames("db1", "table1", "", _ -> Set.of(testPartitionName("part3"), testPartitionName("part4"))))
                .containsExactlyInAnyOrder(testPartitionName("part3"), testPartitionName("part4"));
        assertThat(glueCache.getPartitionNames("db1", "table1", "expression", _ -> Set.of(testPartitionName("c"), testPartitionName("d"))))
                .containsExactlyInAnyOrder(testPartitionName("c"), testPartitionName("d"));

        glueCache.invalidateTable("db1", "table1", true);
        assertThatThrownBy(() -> glueCache.getPartitionNames("db1", "table1", "", _ -> { throw new TrinoException(HIVE_METASTORE_ERROR, "test"); }))
                .isInstanceOf(TrinoException.class)
                .hasMessage("test");
        assertThat(glueCache.getPartitionNames("db1", "table1", "", _ -> Set.of(testPartitionName("after exception"))))
                .containsExactlyInAnyOrder(testPartitionName("after exception"));

        // invalidate table without "cascade" set does not invalidate the partition names
        assertThat(glueCache.getPartitionNames("db1", "table1", "flush", _ -> Set.of(testPartitionName("flush1"), testPartitionName("flush2"))))
                .containsExactlyInAnyOrder(testPartitionName("flush1"), testPartitionName("flush2"));
        glueCache.invalidateTable("db1", "table1", false);
        assertThat(glueCache.getPartitionNames("db1", "table1", "flush", _ -> { throw new RuntimeException(); }))
                .containsExactlyInAnyOrder(testPartitionName("flush1"), testPartitionName("flush2"));

        // invalidate table with cascade invalidates the partition names
        glueCache.invalidateTable("db1", "table1", true);
        assertThat(glueCache.getPartitionNames("db1", "table1", "flush", _ -> Set.of(testPartitionName("flush3"), testPartitionName("flush4"))))
                .containsExactlyInAnyOrder(testPartitionName("flush3"), testPartitionName("flush4"));

        // database invalidation invalidates the partition names
        glueCache.invalidateDatabase("db1");
        assertThat(glueCache.getPartitionNames("db1", "table1", "flush", _ -> Set.of(testPartitionName("flush5"), testPartitionName("flush6"))))
                .containsExactlyInAnyOrder(testPartitionName("flush5"), testPartitionName("flush6"));
    }

    @Test
    void testGetPartitionNamesWithCacheTable()
            throws InterruptedException
    {
        GlueCache glueCache = createGlueCache();
        assertThat(glueCache.getPartitionNames("db1", "table1", "",
                cachePartition -> {
                    cachePartition.accept(testPartition("part1", "initial"));
                    cachePartition.accept(testPartition("part2", "initial"));
                    return Set.of(testPartitionName("part1"), testPartitionName("part2"));
                }))
                .containsExactlyInAnyOrder(testPartitionName("part1"), testPartitionName("part2"));

        // part1 and part2 are already in the cache
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part1"), () -> { throw new RuntimeException(); })).contains(testPartition("part1", "initial"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part2"), () -> { throw new RuntimeException(); })).contains(testPartition("part2", "initial"));
        assertThat(glueCache.getPartitionNames("db1", "table1", "", cachePartition -> { throw new RuntimeException(); }))
                .containsExactlyInAnyOrder(testPartitionName("part1"), testPartitionName("part2"));

        // invalidating the table with "cascade" invalidates partitions and names
        glueCache.invalidateTable("db1", "table1", true);

        // part1 and part2 are still in the cache after invalidating the partition names
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part1"), () -> Optional.of(testPartition("part1", "new")))).contains(testPartition("part1", "new"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part2"), () -> Optional.of(testPartition("part2", "new")))).contains(testPartition("part2", "new"));

        // callback can override the current cached values
        assertThat(glueCache.getPartitionNames("db1", "table1", "",
                cachePartition -> {
                    cachePartition.accept(testPartition("part2", "updated"));
                    cachePartition.accept(testPartition("part3", "initial"));
                    cachePartition.accept(testPartition("part4", "initial"));
                    // in a normal system, part1 and part4 should be returned, but there is no enforcement of this in the cache
                    return Set.of(testPartitionName("part2"), testPartitionName("part3"));
                }))
                .containsExactlyInAnyOrder(testPartitionName("part2"), testPartitionName("part3"));

        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part1"), () -> { throw new RuntimeException(); })).contains(testPartition("part1", "new"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part2"), () -> { throw new RuntimeException(); })).contains(testPartition("part2", "updated"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part3"), () -> { throw new RuntimeException(); })).contains(testPartition("part3", "initial"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part4"), () -> { throw new RuntimeException(); })).contains(testPartition("part4", "initial"));

        // partition invalidation does not invalidate the partition names
        glueCache.invalidatePartition("db1", "table1", testPartitionName("unknown"));
        assertThat(glueCache.getPartitionNames("db1", "table1", "", cachePartition -> { throw new RuntimeException(); }))
                .containsExactlyInAnyOrder(testPartitionName("part2"), testPartitionName("part3"));

        glueCache.invalidateTable("db1", "table1", true);

        // reload all partitions
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part1"), () -> Optional.of(testPartition("part1", "initial")))).contains(testPartition("part1", "initial"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part2"), () -> Optional.of(testPartition("part2", "initial")))).contains(testPartition("part2", "initial"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part3"), () -> Optional.of(testPartition("part3", "initial")))).contains(testPartition("part3", "initial"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part4"), () -> Optional.of(testPartition("part4", "initial")))).contains(testPartition("part4", "initial"));

        // any database invalidation during a bulk load immediately prevents blocks direct caching of full objects
        CountDownLatch inBulkLoad = new CountDownLatch(1);
        CountDownLatch invalidated = new CountDownLatch(1);

        Thread thread = Thread.startVirtualThread(() -> assertThat(glueCache.getPartitionNames("db1", "table1", "",
                cachePartition -> {
                    cachePartition.accept(testPartition("part1", "bulk"));
                    cachePartition.accept(testPartition("part2", "bulk"));
                    inBulkLoad.countDown();
                    awaitUninterruptibly(invalidated);
                    cachePartition.accept(testPartition("part3", "bulk"));
                    cachePartition.accept(testPartition("part4", "bulk"));
                    return Set.of(testPartitionName("part1"), testPartitionName("part2"), testPartitionName("part3"), testPartitionName("part4"));
                }))
                .containsExactlyInAnyOrder(testPartitionName("part1"), testPartitionName("part2"), testPartitionName("part3"), testPartitionName("part4")));

        awaitUninterruptibly(inBulkLoad);
        glueCache.invalidatePartition("db1", "table1", testPartitionName("unknown"));
        invalidated.countDown();
        thread.join();

        // part1 and part2 are now cached with the updated value from the bulk load
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part1"), () -> { throw new RuntimeException(); })).contains(testPartition("part1", "bulk"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part2"), () -> { throw new RuntimeException(); })).contains(testPartition("part2", "bulk"));
        // part3 and part4 are not updated since an invalidation occurred during the bulk load
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part3"), () -> { throw new RuntimeException(); })).contains(testPartition("part3", "initial"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part4"), () -> { throw new RuntimeException(); })).contains(testPartition("part4", "initial"));
    }

    @Test
    void testGetPartition()
    {
        GlueCache glueCache = createGlueCache();
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part1"), Optional::empty))
                .isEmpty();
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part1"), () -> Optional.of(testPartition("part1", "fail"))))
                .isEmpty();
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part1"), () -> { throw new RuntimeException(); }))
                .isEmpty();

        glueCache.invalidatePartition("db1", "table1", testPartitionName("part1"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part1"), () -> Optional.of(testPartition("part1", "initial"))))
                .contains(testPartition("part1", "initial"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part1"), () -> Optional.of(testPartition("part1", "fail"))))
                .contains(testPartition("part1", "initial"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part1"), () -> { throw new RuntimeException(); }))
                .contains(testPartition("part1", "initial"));

        glueCache.invalidatePartition("db1", "table1", testPartitionName("part1"));
        assertThatThrownBy(() -> glueCache.getPartition("db1", "table1", testPartitionName("part1"), () -> { throw new TrinoException(HIVE_METASTORE_ERROR, "test"); }))
                .isInstanceOf(TrinoException.class)
                .hasMessage("test");
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part1"), () -> Optional.of(testPartition("part1", "after exception"))))
                .contains(testPartition("part1", "after exception"));

        // invalidating table without "cascade" does not invalidate the partition
        glueCache.invalidateTable("db1", "table1", false);
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part1"), () -> { throw new RuntimeException(); }))
                .contains(testPartition("part1", "after exception"));

        // invalidating table with cascade invalidates the partition
        glueCache.invalidateTable("db1", "table1", true);
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part1"), () -> Optional.of(testPartition("part1", "table invalidation"))))
                .contains(testPartition("part1", "table invalidation"));

        // database invalidation invalidates the partition
        glueCache.invalidateDatabase("db1");
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part1"), () -> Optional.of(testPartition("part1", "database invalidation"))))
                .contains(testPartition("part1", "database invalidation"));
    }

    @Test
    void testBatchGetPartitions()
            throws InterruptedException
    {
        GlueCache glueCache = createGlueCache();
        assertThatThrownBy(() -> glueCache.batchGetPartitions("db1", "table1", Set.of(testPartitionName("part1"), testPartitionName("part2")),
                (cachePartition, missingPartitions) -> {
                    throw new TrinoException(HIVE_METASTORE_ERROR, "test");
                }))
                .isInstanceOf(TrinoException.class)
                .hasMessage("test");

        assertThat(glueCache.batchGetPartitions("db1", "table1", Set.of(testPartitionName("part1"), testPartitionName("part2")),
                (cachePartition, missingPartitions) -> {
                    assertThat(missingPartitions).containsExactlyInAnyOrder(testPartitionName("part1"), testPartitionName("part2"));
                    cachePartition.accept(testPartition("part1", "initial"));
                    cachePartition.accept(testPartition("part2", "initial"));
                    return Set.of(testPartition("part1", "initial"), testPartition("part2", "initial"));
                }))
                .containsExactlyInAnyOrder(testPartition("part1", "initial"), testPartition("part2", "initial"));

        // part1 and part2 are already in the cache
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part1"), () -> { throw new RuntimeException(); })).contains(testPartition("part1", "initial"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part2"), () -> { throw new RuntimeException(); })).contains(testPartition("part2", "initial"));
        assertThat(glueCache.batchGetPartitions("db1", "table1", Set.of(testPartitionName("part1"), testPartitionName("part2")), (cachePartition, missingPartitions) -> { throw new RuntimeException(); }))
                .containsExactlyInAnyOrder(testPartition("part1", "initial"), testPartition("part2", "initial"));

        // invalidating partition names or table without "cascade" does not invalidate the partitions
        glueCache.invalidateTable("db1", "table1", false);
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part1"), () -> { throw new RuntimeException(); })).contains(testPartition("part1", "initial"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part2"), () -> { throw new RuntimeException(); })).contains(testPartition("part2", "initial"));

        // only missing partitions are fetched
        assertThat(glueCache.batchGetPartitions("db1", "table1", Set.of(testPartitionName("part1"), testPartitionName("part2"), testPartitionName("part3"), testPartitionName("part4")),
                (cachePartition, missingPartitions) -> {
                    assertThat(missingPartitions).containsExactlyInAnyOrder(testPartitionName("part3"), testPartitionName("part4"));
                    cachePartition.accept(testPartition("part3", "initial"));
                    cachePartition.accept(testPartition("part4", "initial"));
                    return Set.of(testPartition("part3", "initial"), testPartition("part4", "initial"));
                }))
                .containsExactlyInAnyOrder(testPartition("part1", "initial"), testPartition("part2", "initial"), testPartition("part3", "initial"), testPartition("part4", "initial"));

        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part1"), () -> { throw new RuntimeException(); })).contains(testPartition("part1", "initial"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part2"), () -> { throw new RuntimeException(); })).contains(testPartition("part2", "initial"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part3"), () -> { throw new RuntimeException(); })).contains(testPartition("part3", "initial"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part4"), () -> { throw new RuntimeException(); })).contains(testPartition("part4", "initial"));

        // any database invalidation during a bulk load immediately prevents blocks direct caching of full objects
        glueCache.invalidateTable("db1", "table1", true);
        CountDownLatch inBulkLoad = new CountDownLatch(1);
        CountDownLatch invalidated = new CountDownLatch(1);

        Thread thread = Thread.startVirtualThread(() -> assertThat(glueCache.batchGetPartitions(
                "db1",
                "table1",
                Set.of(testPartitionName("part1"), testPartitionName("part2"), testPartitionName("part3"), testPartitionName("part4")),
                (cachePartition, missingPartitions) -> {
                    assertThat(missingPartitions).containsExactlyInAnyOrder(testPartitionName("part1"), testPartitionName("part2"), testPartitionName("part3"), testPartitionName("part4"));
                    cachePartition.accept(testPartition("part1", "bulk"));
                    cachePartition.accept(testPartition("part2", "bulk"));
                    inBulkLoad.countDown();
                    awaitUninterruptibly(invalidated);
                    cachePartition.accept(testPartition("part3", "bulk"));
                    cachePartition.accept(testPartition("part4", "bulk"));
                    return Set.of(testPartition("part1", "bulk"), testPartition("part2", "bulk"), testPartition("part3", "bulk"), testPartition("part4", "bulk"));
                }))
                .containsExactlyInAnyOrder(testPartition("part1", "bulk"), testPartition("part2", "bulk"), testPartition("part3", "bulk"), testPartition("part4", "bulk")));

        awaitUninterruptibly(inBulkLoad);
        glueCache.invalidatePartition("db1", "table1", testPartitionName("unknown"));
        invalidated.countDown();
        thread.join();

        // part1 and part2 are now cached with the updated value from the bulk load
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part1"), () -> { throw new RuntimeException(); })).contains(testPartition("part1", "bulk"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part2"), () -> { throw new RuntimeException(); })).contains(testPartition("part2", "bulk"));
        // part3 and part4 are not updated since an invalidation occurred during the bulk load
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part3"), () -> Optional.of(testPartition("part3", "after bulk")))).contains(testPartition("part3", "after bulk"));
        assertThat(glueCache.getPartition("db1", "table1", testPartitionName("part4"), () -> Optional.of(testPartition("part4", "after bulk")))).contains(testPartition("part4", "after bulk"));
    }

    @Test
    void testGetPartitionColumnStatistics()
    {
        GlueCache glueCache = createGlueCache();

        // column stats missing in the backend system, and are simply missing from the result map
        assertThat(glueCache.getPartitionColumnStatistics("db", "table", testPartitionName("part1"), Set.of("col1", "col2"),
                missingColumns -> {
                    assertThat(missingColumns).containsExactlyInAnyOrder("col1", "col2");
                    return Map.of();
                }))
                .isEmpty();

        // stats that fail to load are cached, callback is not invoked
        assertThat(glueCache.getPartitionColumnStatistics("db", "table", testPartitionName("part1"), Set.of("col1", "col2"), missingColumns -> { throw new RuntimeException(); }))
                .isEmpty();

        glueCache.invalidatePartition("db", "table", testPartitionName("part1"));
        assertThat(glueCache.getPartitionColumnStatistics("db", "table", testPartitionName("part1"), Set.of("col1", "col2"),
                missingColumns -> {
                    assertThat(missingColumns).containsExactlyInAnyOrder("col1", "col2");
                    return Map.of("col1", testStats(1), "col2", testStats(2));
                }))
                .containsExactlyInAnyOrderEntriesOf(Map.of("col1", testStats(1), "col2", testStats(2)));

        assertThat(glueCache.getPartitionColumnStatistics("db", "table", testPartitionName("part1"), Set.of("col1", "col2"), missingColumns -> { throw new RuntimeException(); }))
                .containsExactlyInAnyOrderEntriesOf(Map.of("col1", testStats(1), "col2", testStats(2)));

        // additional columns can be requested and only missing columns are fetched
        assertThat(glueCache.getPartitionColumnStatistics("db", "table", testPartitionName("part1"), Set.of("col1", "col2", "col3", "col4"),
                missingColumns -> {
                    assertThat(missingColumns).containsExactlyInAnyOrder("col3", "col4");
                    return Map.of("col3", testStats(3), "col4", testStats(4));
                }))
                .containsExactlyInAnyOrderEntriesOf(Map.of("col1", testStats(1), "col2", testStats(2), "col3", testStats(3), "col4", testStats(4)));
        assertThat(glueCache.getPartitionColumnStatistics("db", "table", testPartitionName("part1"), Set.of("col1", "col2", "col3", "col4"), missingColumns -> { throw new RuntimeException(); }))
                .containsExactlyInAnyOrderEntriesOf(Map.of("col1", testStats(1), "col2", testStats(2), "col3", testStats(3), "col4", testStats(4)));

        // partition invalidation invalidates the column stats
        glueCache.invalidatePartition("db", "table", testPartitionName("part1"));
        assertThat(glueCache.getPartitionColumnStatistics("db", "table", testPartitionName("part1"), Set.of("col1", "col2"),
                missingColumns -> {
                    assertThat(missingColumns).containsExactlyInAnyOrder("col1", "col2");
                    return Map.of("col1", testStats(11), "col2", testStats(22));
                }))
                .isEqualTo(Map.of("col1", testStats(11), "col2", testStats(22)));

        // table invalidation does not invalidate the column stats unless cascade is true
        glueCache.invalidateTable("db", "table", false);
        assertThat(glueCache.getPartitionColumnStatistics("db", "table", testPartitionName("part1"), Set.of("col1", "col2"), missingColumns -> { throw new RuntimeException(); }))
                .isEqualTo(Map.of("col1", testStats(11), "col2", testStats(22)));
        glueCache.invalidateTable("db", "table", true);
        assertThat(glueCache.getPartitionColumnStatistics("db", "table", testPartitionName("part1"), Set.of("col1", "col2"),
                missingColumns -> {
                    assertThat(missingColumns).containsExactlyInAnyOrder("col1", "col2");
                    return Map.of("col1", testStats(111), "col2", testStats(222));
                }))
                .isEqualTo(Map.of("col1", testStats(111), "col2", testStats(222)));

        // database invalidation invalidates the column stats
        glueCache.invalidateDatabase("db");
        assertThat(glueCache.getPartitionColumnStatistics("db", "table", testPartitionName("part1"), Set.of("col1", "col2"),
                missingColumns -> {
                    assertThat(missingColumns).containsExactlyInAnyOrder("col1", "col2");
                    return Map.of("col1", testStats(1111), "col2", testStats(2222));
                }))
                .isEqualTo(Map.of("col1", testStats(1111), "col2", testStats(2222)));
    }

    @Test
    void testGetAllFunctions()
    {
        GlueCache glueCache = createGlueCache();
        assertThat(glueCache.getAllFunctions("db1", () -> Set.of(testFunction("x"), testFunction("y")))).containsExactlyInAnyOrder(testFunction("x"), testFunction("y"));
        assertThat(glueCache.getAllFunctions("db1", () -> Set.of(testFunction("fail")))).containsExactlyInAnyOrder(testFunction("x"), testFunction("y"));
        assertThat(glueCache.getAllFunctions("db1", () -> { throw new RuntimeException(); })).containsExactlyInAnyOrder(testFunction("x"), testFunction("y"));

        // invalidation of a single function invalidates the db list
        glueCache.invalidateFunction("db1", "unknown");
        assertThat(glueCache.getAllFunctions("db1", () -> Set.of(testFunction("a"), testFunction("b")))).containsExactlyInAnyOrder(testFunction("a"), testFunction("b"));

        // database invalidation invalidates the function list
        glueCache.invalidateDatabase("db1");
        assertThat(glueCache.getAllFunctions("db1", () -> Set.of(testFunction("c"), testFunction("d")))).containsExactlyInAnyOrder(testFunction("c"), testFunction("d"));

        glueCache.invalidateFunction("db1", "unknown");
        assertThatThrownBy(() -> glueCache.getAllFunctions("db1", () -> { throw new TrinoException(HIVE_METASTORE_ERROR, "test"); }))
                .isInstanceOf(TrinoException.class)
                .hasMessage("test");
        assertThat(glueCache.getAllFunctions("db1", () -> Set.of(testFunction("after exception")))).containsExactlyInAnyOrder(testFunction("after exception"));
    }

    @Test
    void testGetFunction()
    {
        GlueCache glueCache = createGlueCache();
        assertThat(glueCache.getFunction("db1", "func1", () -> List.of(testFunction("a"), testFunction("b"))))
                .containsExactlyInAnyOrder(testFunction("a"), testFunction("b"));
        assertThat(glueCache.getFunction("db1", "func1", () -> List.of(testFunction("fail"))))
                .containsExactlyInAnyOrder(testFunction("a"), testFunction("b"));
        assertThat(glueCache.getFunction("db1", "func1", () -> { throw new RuntimeException(); }))
                .containsExactlyInAnyOrder(testFunction("a"), testFunction("b"));

        glueCache.invalidateFunction("db1", "func1");
        assertThat(glueCache.getFunction("db1", "func1", () -> List.of(testFunction("c"), testFunction("d"))))
                .containsExactlyInAnyOrder(testFunction("c"), testFunction("d"));

        // database invalidation invalidates the function
        glueCache.invalidateDatabase("db1");
        assertThat(glueCache.getFunction("db1", "func1", () -> List.of(testFunction("e"), testFunction("f"))))
                .containsExactlyInAnyOrder(testFunction("e"), testFunction("f"));
    }

    private static Database testDatabase(String name)
    {
        return testDatabase(name, "no-owner");
    }

    private static Database testDatabase(String name, String owner)
    {
        return Database.builder()
                .setDatabaseName(name)
                .setOwnerName(Optional.of(owner))
                .setOwnerType(Optional.of(PrincipalType.USER))
                .build();
    }

    private static TableInfo testTableInfo(String schemaName, String tableName)
    {
        return new TableInfo(new SchemaTableName(schemaName, tableName), TableInfo.ExtendedRelationType.TABLE);
    }

    private static Table testTable(String schemaName, String tableName)
    {
        return testTable(schemaName, tableName, "no-owner");
    }

    private static Table testTable(String schemaName, String tableName, String owner)
    {
        return Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setOwner(Optional.of(owner))
                .setTableType("table")
                .withStorage(storage -> storage.setStorageFormat(StorageFormat.NULL_STORAGE_FORMAT))
                .build();
    }

    private static PartitionName testPartitionName(String name)
    {
        return new PartitionName(List.of(name, name));
    }

    private static Partition testPartition(String partitionName, String version)
    {
        return testPartition("db1", "table1", partitionName, version);
    }

    private static Partition testPartition(String schemaName, String tableName, String partitionName, String version)
    {
        return Partition.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setValues(List.of(partitionName, partitionName))
                .withStorage(storage -> storage.setStorageFormat(StorageFormat.NULL_STORAGE_FORMAT))
                .setColumns(List.of(new Column(version, HiveType.HIVE_STRING, Optional.empty(), Map.of())))
                .build();
    }

    private static HiveColumnStatistics testStats(int columnNumber)
    {
        return HiveColumnStatistics.createBinaryColumnStatistics(OptionalLong.of(columnNumber + 1), OptionalDouble.of(columnNumber + 2), OptionalLong.of(columnNumber + 3));
    }

    private static LanguageFunction testFunction(String signatureToken)
    {
        return new LanguageFunction(signatureToken, "", List.of(), Optional.empty());
    }
}
