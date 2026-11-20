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
package io.trino.plugin.deltalake;

import com.google.common.base.Stopwatch;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Set;
import java.util.function.BiConsumer;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDeltaLakeTableLocationChanged
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA = "smoke_test";

    // Cannot be too small, as implicit (time-based) cache invalidation can mask issues. Cannot be too big as the tests wait for cache to be outdated.
    private static final int TEST_METADATA_CACHE_TTL_SECONDS = 5;

    @TempDir
    static Path catalogDir;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DeltaLakeQueryRunner.builder(SCHEMA)
                .addDeltaProperty("fs.hadoop.enabled", "true")
                .addDeltaProperty("hive.metastore.catalog.dir", catalogDir.toUri().toString())
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                .addDeltaProperty("delta.metadata.cache-ttl", TEST_METADATA_CACHE_TTL_SECONDS + "s")
                .addDeltaProperty("hive.metastore-cache-ttl", TEST_METADATA_CACHE_TTL_SECONDS + "s")
                .build();
    }

    @Test
    void testDeltaLakeTableLocationChanged()
            throws Exception
    {
        testDeltaLakeTableLocationChanged(true, false, false);
    }

    @Test
    void testDeltaLakeTableLocationChangedSameVersionNumber()
            throws Exception
    {
        testDeltaLakeTableLocationChanged(false, false, false);
    }

    @Test
    void testDeltaLakeTableLocationChangedPartitioned()
            throws Exception
    {
        testDeltaLakeTableLocationChanged(true, true, false);
        testDeltaLakeTableLocationChanged(true, false, true);
        testDeltaLakeTableLocationChanged(true, true, true);
    }

    private void testDeltaLakeTableLocationChanged(boolean fewerEntries, boolean firstPartitioned, boolean secondPartitioned)
            throws Exception
    {
        // Create a table with a bunch of transaction log entries
        String tableName = "test_table_location_changed_" + randomNameSuffix();
        String initialLocation = location(tableName);
        assertUpdate("CREATE TABLE %s (a_number int, a_string varchar) WITH (location = '%s' %s)".formatted(
                tableName,
                initialLocation,
                firstPartitioned ? ", partitioned_by = ARRAY['a_number']" : ""));

        BiConsumer<QueryRunner, String> insertABunchOfRows = (queryRunner, prefix) -> {
            queryRunner.execute("INSERT INTO %s (a_number, a_string) VALUES (1, '%s one')".formatted(tableName, prefix));
            queryRunner.execute("INSERT INTO %s (a_number, a_string) VALUES (2, '%s two')".formatted(tableName, prefix));
            queryRunner.execute("INSERT INTO %s (a_number, a_string) VALUES (3, '%s tree')".formatted(tableName, prefix));
            queryRunner.execute("INSERT INTO %s (a_number, a_string) VALUES (4, '%s four')".formatted(tableName, prefix));
        };

        insertABunchOfRows.accept(getQueryRunner(), "first");

        MaterializedResult initialData = computeActual("SELECT * FROM " + tableName);
        assertThat(initialData.getMaterializedRows()).hasSize(4);

        MaterializedResult expectedDataAfterChange;
        String newLocation;
        try (QueryRunner independentQueryRunner = createQueryRunner()) {
            // Change table's location without main Delta Lake connector (main query runner) knowing about this
            newLocation = location("test_table_location_changed_new_" + randomNameSuffix());

            independentQueryRunner.execute("DROP TABLE " + tableName);
            independentQueryRunner.execute("CREATE TABLE %s (a_number int, a_string varchar, another_string varchar) WITH (location = '%s' %s) ".formatted(
                    tableName,
                    newLocation,
                    secondPartitioned ? ", partitioned_by = ARRAY['a_number']" : ""));

            if (fewerEntries) {
                // Have fewer transaction log entries so that version mismatch is more apparent (but easier to detect)
                independentQueryRunner.execute("INSERT INTO %s VALUES (1, 'second one', 'third column')".formatted(tableName));
            }
            else {
                insertABunchOfRows.accept(independentQueryRunner, "second");
            }

            expectedDataAfterChange = independentQueryRunner.execute("SELECT * FROM " + tableName);
            assertThat(expectedDataAfterChange.getMaterializedRows()).hasSize(fewerEntries ? 1 : 4);
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        while (true) {
            MaterializedResult currentVisibleData = computeActual("SELECT * FROM " + tableName);
            if (Set.copyOf(currentVisibleData.getMaterializedRows()).equals(Set.copyOf(expectedDataAfterChange.getMaterializedRows()))) {
                // satisfied
                break;
            }
            if (!Set.copyOf(currentVisibleData.getMaterializedRows()).equals(Set.copyOf(initialData.getMaterializedRows()))) {
                throw new AssertionError("Unexpected result when reading table: %s,\n expected either initialData: %s\n or expectedDataAfterChange: %s".formatted(
                        currentVisibleData,
                        initialData,
                        expectedDataAfterChange));
            }

            if (stopwatch.elapsed(SECONDS) > TEST_METADATA_CACHE_TTL_SECONDS + 10) {
                throw new RuntimeException("Timed out waiting on table to reflect new data from new location");
            }

            SECONDS.sleep(1);
        }

        // Verify table schema gets reflected correctly
        String qualifiedTableName = "%s.%s.%s".formatted(getSession().getCatalog().orElseThrow(), SCHEMA, tableName);
        assertThat(computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo("" +
                        "CREATE TABLE " + qualifiedTableName + " (\n" +
                        "   a_number integer,\n" +
                        "   a_string varchar,\n" +
                        "   another_string varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   location = '" + newLocation + "'" + (secondPartitioned ? "," : "") + "\n" +
                        (secondPartitioned ? "   partitioned_by = ARRAY['a_number']\n" : "") +
                        ")");
    }

    private static String location(String tableName)
    {
        return catalogDir.resolve(tableName).toUri().toString();
    }
}
