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
package io.trino.plugin.hidden.partitioning;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.hive.HiveQueryRunner.HIVE_CATALOG;
import static io.trino.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tpch.TpchTable.NATION;

public class TestHiddenPartitioningPartitionTransforms
        extends AbstractTestQueryFramework
{
    private static final String DAY_SPEC = "{\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"event_date\",\n" +
            "    \"transform\" : \"day\",\n" +
            "    \"source-name\" : \"event_time\"\n" +
            "  } ]\n" +
            "}";
    private static final String HOUR_SPEC = "{\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"event_hour\",\n" +
            "    \"transform\" : \"hour\",\n" +
            "    \"source-name\" : \"event_time\"\n" +
            "  } ]\n" +
            "}";

    private final Session session;

    public TestHiddenPartitioningPartitionTransforms()
    {
        session = getHiveSession();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiddenPartitioningQueryRunner.builder()
                .setInitialTables(ImmutableList.of(NATION)) // create TPCH schema
                .setHiveProperties(ImmutableMap.of("hive.partition-spec-enabled", "true"))
                .build();
    }

    @Test
    public void testDayTransformEqualityFilter()
    {
        createTableDaySpec(session, "dayTransformEqualityFilter");

        // Purposely put some rows with the event_time in the wrong partitions, to test the hidden partition filtering
        assertUpdate(session, "INSERT INTO dayTransformEqualityFilter(id, event_time, event_date) VALUES (1, timestamp '2020-04-20 00:00:00', date '2020-04-19')", 1);
        assertUpdate(session, "INSERT INTO dayTransformEqualityFilter(id, event_time, event_date) VALUES (2, timestamp '2020-04-20 00:00:00', date '2020-04-20')", 1);
        assertUpdate(session, "INSERT INTO dayTransformEqualityFilter(id, event_time, event_date) VALUES (3, timestamp '2020-04-20 00:00:00', date '2020-04-21')", 1);

        // Partition spec transform of event_time ensures only 1 result
        String query = "SELECT id FROM dayTransformEqualityFilter WHERE event_time = timestamp '2020-04-20 00:00:00'";
        assertQuery(session, query, "select 2");

        assertUpdate(session, "DELETE FROM dayTransformEqualityFilter");

        assertUpdate(session, "INSERT INTO dayTransformEqualityFilter(id, event_time, event_date) VALUES (1, timestamp '2020-04-19 00:00:00', date '2020-04-19')", 1);
        assertUpdate(session, "INSERT INTO dayTransformEqualityFilter(id, event_time, event_date) VALUES (2, timestamp '2020-04-19 00:00:00', date '2020-04-20')", 1);
        assertUpdate(session, "INSERT INTO dayTransformEqualityFilter(id, event_time, event_date) VALUES (3, timestamp '2020-04-21 00:00:00', date '2020-04-21')", 1);

        query = "SELECT id FROM dayTransformEqualityFilter WHERE event_time = timestamp '2020-04-19 00:00:00' OR event_time = timestamp '2020-04-21 00:00:00'";
        assertQuery(session, query, "VALUES (1), (3)");

        query = "SELECT id FROM dayTransformEqualityFilter WHERE event_time IN (timestamp '2020-04-19 00:00:00', timestamp '2020-04-21 00:00:00')";
        assertQuery(session, query, "VALUES (1), (3)");

        // '2020-04-20' partition included because it could still have timestamps satisfying the predicate, e.g. '2020-04-20 11:24:32'
        query = "SELECT id FROM dayTransformEqualityFilter WHERE event_time != timestamp '2020-04-20 00:00:00'";
        assertQuery(session, query, "VALUES (1), (2), (3)");

        query = "SELECT id FROM dayTransformEqualityFilter WHERE event_time NOT IN (timestamp '2020-04-20 00:00:00')";
        assertQuery(session, query, "VALUES (1), (2), (3)");

        query = "SELECT count(*) FROM dayTransformEqualityFilter WHERE event_time = timestamp '2020-04-19 00:00:00' AND event_time = timestamp '2020-04-21 00:00:00'";
        assertQuery(session, query, "SELECT 0");

        query = "SELECT count(*) FROM dayTransformEqualityFilter WHERE event_time IS NULL";
        assertQuery(session, query, "SELECT 0");

        query = "SELECT count(*) FROM dayTransformEqualityFilter WHERE event_time IS NOT NULL";
        assertQuery(session, query, "SELECT 3");

        assertUpdate(session, "DROP TABLE dayTransformEqualityFilter");
    }

    @Test
    public void testDayTransformRangeFilter()
    {
        createTableDaySpec(session, "dayTransformRangeFilter");

        assertUpdate(session, "INSERT INTO dayTransformRangeFilter(id, event_time, event_date) VALUES (1, timestamp '2020-04-20 00:00:00', date '2020-04-18')", 1);
        assertUpdate(session, "INSERT INTO dayTransformRangeFilter(id, event_time, event_date) VALUES (2, timestamp '2020-04-20 00:00:00', date '2020-04-19')", 1);
        assertUpdate(session, "INSERT INTO dayTransformRangeFilter(id, event_time, event_date) VALUES (3, timestamp '2020-04-20 00:00:00', date '2020-04-20')", 1);
        assertUpdate(session, "INSERT INTO dayTransformRangeFilter(id, event_time, event_date) VALUES (4, timestamp '2020-04-20 00:00:00', date '2020-04-21')", 1);

        String query = "SELECT id FROM dayTransformRangeFilter WHERE event_time > timestamp '2020-04-18 00:00:00' AND event_time <= timestamp '2020-04-21 23:59:59'";
        assertQuery(session, query, "VALUES (1), (2), (3), (4)");

        query = "SELECT id FROM dayTransformRangeFilter WHERE event_time > timestamp '2020-04-19 00:00:00' AND event_time < timestamp '2020-04-20 23:59:59'";
        assertQuery(session, query, "VALUES (2), (3)");

        query = "SELECT id FROM dayTransformRangeFilter WHERE event_time >= timestamp '2020-04-19 00:00:00' AND event_time <= timestamp '2020-04-20 23:59:59'";
        assertQuery(session, query, "VALUES (2), (3)");

        query = "SELECT count(*) FROM dayTransformRangeFilter WHERE event_time > timestamp '2020-04-19 23:59:59' AND event_time < timestamp '2020-04-20 00:00:00'";
        assertQuery(session, query, "SELECT 0");

        // Iceberg has microsecond precision whereas Presto only has millisecond precision, so the partition with 23:59:59.999 is still included in these cases
        query = "SELECT count(*) FROM dayTransformRangeFilter WHERE event_time > timestamp '2020-04-19 23:59:59.999' AND event_time <= timestamp '2020-04-20 00:00:00'";
        assertQuery(session, query, "SELECT 2");
        query = "SELECT id FROM dayTransformRangeFilter WHERE event_time > timestamp '2020-04-18 23:59:59.999' AND event_time < timestamp '2020-04-21 00:00:00'";
        assertQuery(session, query, "VALUES (1), (2), (3)");

        query = "SELECT id FROM dayTransformRangeFilter WHERE event_time >= timestamp '2020-04-18 23:59:59' AND event_time <= timestamp '2020-04-20 00:00:00'";
        assertQuery(session, query, "VALUES (1), (2), (3)");

        assertUpdate(session, "DELETE FROM dayTransformRangeFilter");

        assertUpdate(session, "INSERT INTO dayTransformRangeFilter(id, event_time, event_date) VALUES (1, timestamp '2020-04-18 00:00:00', date '2020-04-18')", 1);
        assertUpdate(session, "INSERT INTO dayTransformRangeFilter(id, event_time, event_date) VALUES (2, timestamp '2020-04-18 00:00:00', date '2020-04-19')", 1);
        assertUpdate(session, "INSERT INTO dayTransformRangeFilter(id, event_time, event_date) VALUES (3, timestamp '2020-04-20 00:00:00', date '2020-04-20')", 1);
        assertUpdate(session, "INSERT INTO dayTransformRangeFilter(id, event_time, event_date) VALUES (4, timestamp '2020-04-20 00:00:00', date '2020-04-21')", 1);

        // disjoint
        query = "SELECT id FROM dayTransformRangeFilter " +
                "WHERE (event_time >= timestamp '2020-04-18 00:00:00' AND event_time < timestamp '2020-04-19 00:00:00') " +
                "OR (event_time >= timestamp '2020-04-20 00:00:00' AND event_time < timestamp '2020-04-21 00:00:00')";
        assertQuery(session, query, "VALUES (1), (3)");

        // overlapping
        query = "SELECT id FROM dayTransformRangeFilter " +
                "WHERE (event_time >= timestamp '2020-04-18 00:00:00' AND event_time < timestamp '2020-04-19 00:00:00') " +
                "OR (event_time > timestamp '2020-04-18 00:00:00' AND event_time < timestamp '2020-04-20 00:00:00')";
        assertQuery(session, query, "VALUES (1), (2)");

        assertUpdate(session, "DROP TABLE dayTransformRangeFilter");
    }

    @Test
    public void testDayTransformFilterOnPartitionCol()
    {
        createTableDaySpec(session, "dayTransformFilterOnPartitionCol");

        assertUpdate(session, "INSERT INTO dayTransformFilterOnPartitionCol(id, event_time, event_date) VALUES (1, timestamp '2020-04-19 00:00:00', date '2020-04-18')", 1);
        assertUpdate(session, "INSERT INTO dayTransformFilterOnPartitionCol(id, event_time, event_date) VALUES (2, timestamp '2020-04-18 11:00:00', date '2020-04-19')", 1);
        assertUpdate(session, "INSERT INTO dayTransformFilterOnPartitionCol(id, event_time, event_date) VALUES (3, timestamp '2020-04-20 00:00:00', date '2020-04-20')", 1);
        assertUpdate(session, "INSERT INTO dayTransformFilterOnPartitionCol(id, event_time, event_date) VALUES (4, timestamp '2020-04-20 00:00:00', date '2020-04-21')", 1);

        // event_time filter more restrictive
        String query = "SELECT id FROM dayTransformFilterOnPartitionCol WHERE event_time > timestamp '2020-04-18 00:00:00' AND event_time <= timestamp '2020-04-20 23:59:59'" +
                " AND event_date >= date '2020-04-18'";
        assertQuery(session, query, "VALUES (1), (2), (3)");

        // event_date filter more restrictive
        query = "SELECT id FROM dayTransformFilterOnPartitionCol WHERE event_time > timestamp '2020-04-18 00:00:00' AND event_time < timestamp '2020-04-21 23:59:59'" +
                " AND event_date >= date '2020-04-18' AND event_date < date '2020-04-20'";
        assertQuery(session, query, "VALUES (1), (2)");

        assertUpdate(session, "DROP TABLE dayTransformFilterOnPartitionCol");
    }

    @Test
    public void testHourTransformEqualityFilter()
    {
        createTableHourSpec(session, "hourTransformEqualityFilter");

        assertUpdate(session, "INSERT INTO hourTransformEqualityFilter(id, event_time, event_hour) VALUES (1, timestamp '2020-04-20 20:00:00', timestamp '2020-04-20 19:00:00')", 1);
        assertUpdate(session, "INSERT INTO hourTransformEqualityFilter(id, event_time, event_hour) VALUES (2, timestamp '2020-04-20 20:00:00', timestamp '2020-04-20 20:00:00')", 1);
        assertUpdate(session, "INSERT INTO hourTransformEqualityFilter(id, event_time, event_hour) VALUES (3, timestamp '2020-04-20 20:00:00', timestamp '2020-04-20 21:00:00')", 1);

        String query = "SELECT id FROM hourTransformEqualityFilter WHERE event_time = timestamp '2020-04-20 20:00:00'";
        assertQuery(session, query, "select 2");

        assertUpdate(session, "DELETE FROM hourTransformEqualityFilter");

        assertUpdate(session, "INSERT INTO hourTransformEqualityFilter(id, event_time, event_hour) VALUES (1, timestamp '2020-04-20 19:00:00', timestamp '2020-04-20 19:00:00')", 1);
        assertUpdate(session, "INSERT INTO hourTransformEqualityFilter(id, event_time, event_hour) VALUES (2, timestamp '2020-04-20 19:00:00', timestamp '2020-04-20 20:00:00')", 1);
        assertUpdate(session, "INSERT INTO hourTransformEqualityFilter(id, event_time, event_hour) VALUES (3, timestamp '2020-04-20 21:00:00', timestamp '2020-04-20 21:00:00')", 1);

        query = "SELECT id FROM hourTransformEqualityFilter WHERE event_time = timestamp '2020-04-20 19:00:00' OR event_time = timestamp '2020-04-20 21:00:00'";
        assertQuery(session, query, "VALUES (1), (3)");

        query = "SELECT id FROM hourTransformEqualityFilter WHERE event_time IN (timestamp '2020-04-20 19:00:00', timestamp '2020-04-20 21:00:00')";
        assertQuery(session, query, "VALUES (1), (3)");

        // '2020-04-20 20:00:00' partition included because it could still have timestamps satisfying the predicate, e.g. '2020-04-20 20:24:32'
        query = "SELECT id FROM hourTransformEqualityFilter WHERE event_time != timestamp '2020-04-20 20:00:00'";
        assertQuery(session, query, "VALUES (1), (2), (3)");

        query = "SELECT id FROM hourTransformEqualityFilter WHERE event_time NOT IN (timestamp '2020-04-20 20:00:00')";
        assertQuery(session, query, "VALUES (1), (2), (3)");

        query = "SELECT count(*) FROM hourTransformEqualityFilter WHERE event_time = timestamp '2020-04-20 19:00:00' AND event_time = timestamp '2020-04-20 21:00:00'";
        assertQuery(session, query, "SELECT 0");

        query = "SELECT count(*) FROM hourTransformEqualityFilter WHERE event_time IS NULL";
        assertQuery(session, query, "SELECT 0");

        query = "SELECT count(*) FROM hourTransformEqualityFilter WHERE event_time IS NOT NULL";
        assertQuery(session, query, "SELECT 3");

        assertUpdate(session, "DROP TABLE hourTransformEqualityFilter");
    }

    @Test
    public void testHourTransformRangeFilter()
    {
        createTableHourSpec(session, "hourTransformRangeFilter");

        assertUpdate(session, "INSERT INTO hourTransformRangeFilter(id, event_time, event_hour) VALUES (1, timestamp '2020-04-20 20:00:00', timestamp '2020-04-20 18:00:00')", 1);
        assertUpdate(session, "INSERT INTO hourTransformRangeFilter(id, event_time, event_hour) VALUES (2, timestamp '2020-04-20 20:00:00', timestamp '2020-04-20 19:00:00')", 1);
        assertUpdate(session, "INSERT INTO hourTransformRangeFilter(id, event_time, event_hour) VALUES (3, timestamp '2020-04-20 20:00:00', timestamp '2020-04-20 20:00:00')", 1);
        assertUpdate(session, "INSERT INTO hourTransformRangeFilter(id, event_time, event_hour) VALUES (4, timestamp '2020-04-20 20:00:00', timestamp '2020-04-20 21:00:00')", 1);

        String query = "SELECT id FROM hourTransformRangeFilter WHERE event_time > timestamp '2020-04-20 18:00:00' AND event_time <= timestamp '2020-04-20 21:59:59'";
        assertQuery(session, query, "VALUES (1), (2), (3), (4)");

        query = "SELECT id FROM hourTransformRangeFilter WHERE event_time > timestamp '2020-04-20 19:00:00' AND event_time < timestamp '2020-04-20 20:59:59'";
        assertQuery(session, query, "VALUES (2), (3)");

        query = "SELECT id FROM hourTransformRangeFilter WHERE event_time >= timestamp '2020-04-20 19:00:00' AND event_time <= timestamp '2020-04-20 20:59:59'";
        assertQuery(session, query, "VALUES (2), (3)");

        query = "SELECT count(*) FROM hourTransformRangeFilter WHERE event_time > timestamp '2020-04-20 19:59:59' AND event_time < timestamp '2020-04-20 20:00:00'";
        assertQuery(session, query, "SELECT 0");

        // Iceberg has microsecond precision whereas Presto only has millisecond precision, so the partition with 23:59:59.999 is still included in these cases
        query = "SELECT id FROM hourTransformRangeFilter WHERE event_time > timestamp '2020-04-20 18:59:59.999' AND event_time < timestamp '2020-04-20 21:00:00'";
        assertQuery(session, query, "VALUES (1), (2), (3)");

        query = "SELECT id FROM hourTransformRangeFilter WHERE event_time >= timestamp '2020-04-20 18:59:59' AND event_time <= timestamp '2020-04-20 20:00:00'";
        assertQuery(session, query, "VALUES (1), (2), (3)");

        assertUpdate(session, "DELETE FROM hourTransformRangeFilter");

        assertUpdate(session, "INSERT INTO hourTransformRangeFilter(id, event_time, event_hour) VALUES (1, timestamp '2020-04-20 18:00:00', timestamp '2020-04-20 18:00:00')", 1);
        assertUpdate(session, "INSERT INTO hourTransformRangeFilter(id, event_time, event_hour) VALUES (2, timestamp '2020-04-20 18:00:00', timestamp '2020-04-20 19:00:00')", 1);
        assertUpdate(session, "INSERT INTO hourTransformRangeFilter(id, event_time, event_hour) VALUES (3, timestamp '2020-04-20 20:00:00', timestamp '2020-04-20 20:00:00')", 1);
        assertUpdate(session, "INSERT INTO hourTransformRangeFilter(id, event_time, event_hour) VALUES (4, timestamp '2020-04-20 20:00:00', timestamp '2020-04-20 21:00:00')", 1);

        // disjoint
        query = "SELECT id FROM hourTransformRangeFilter " +
                "WHERE (event_time >= timestamp '2020-04-20 18:00:00' AND event_time < timestamp '2020-04-20 19:00:00') " +
                "OR (event_time >= timestamp '2020-04-20 20:00:00' AND event_time < timestamp '2020-04-20 21:00:00')";
        assertQuery(session, query, "VALUES (1), (3)");

        // overlapping
        query = "SELECT id FROM hourTransformRangeFilter " +
                "WHERE (event_time >= timestamp '2020-04-20 18:00:00' AND event_time < timestamp '2020-04-20 19:00:00') " +
                "OR (event_time > timestamp '2020-04-20 18:00:00' AND event_time < timestamp '2020-04-20 20:00:00')";
        assertQuery(session, query, "VALUES (1), (2)");

        assertUpdate(session, "DROP TABLE hourTransformRangeFilter");
    }

    @Test
    public void testHourTransformFilterOnPartitionCol()
    {
        createTableHourSpec(session, "hourTransformFilterOnPartitionCol");

        assertUpdate(session, "INSERT INTO hourTransformFilterOnPartitionCol(id, event_time, event_hour) VALUES (1, timestamp '2020-04-20 19:00:00', timestamp '2020-04-20 18:00:00')", 1);
        assertUpdate(session, "INSERT INTO hourTransformFilterOnPartitionCol(id, event_time, event_hour) VALUES (2, timestamp '2020-04-20 18:33:00', timestamp '2020-04-20 19:00:00')", 1);
        assertUpdate(session, "INSERT INTO hourTransformFilterOnPartitionCol(id, event_time, event_hour) VALUES (3, timestamp '2020-04-20 20:00:00', timestamp '2020-04-20 20:00:00')", 1);
        assertUpdate(session, "INSERT INTO hourTransformFilterOnPartitionCol(id, event_time, event_hour) VALUES (4, timestamp '2020-04-20 20:00:00', timestamp '2020-04-20 21:00:00')", 1);

        // event_time filter more restrictive
        String query = "SELECT id FROM hourTransformFilterOnPartitionCol WHERE event_time > timestamp '2020-04-20 18:00:00' AND event_time < timestamp '2020-04-20 20:59:59'" +
                " AND event_hour >= timestamp '2020-04-20 18:00:00'";
        assertQuery(session, query, "VALUES (1), (2), (3)");

        // event_hour filter more restrictive
        query = "SELECT id FROM hourTransformFilterOnPartitionCol WHERE event_time > timestamp '2020-04-20 18:00:00' AND event_time < timestamp '2020-04-20 21:59:59'" +
                " AND event_hour >= timestamp '2020-04-20 18:00:00' AND event_hour < timestamp '2020-04-20 20:00:00'";
        assertQuery(session, query, "VALUES (1), (2)");

        assertUpdate(session, "DROP TABLE hourTransformFilterOnPartitionCol");
    }

    @Test
    public void testFunctions()
    {
        createTableHourSpec(session, "functions_partition");

        assertUpdate(session, "INSERT INTO functions_partition(id, event_time, event_hour) VALUES (1, timestamp '2020-04-20 20:00:00', timestamp '2020-04-20 18:00:00')", 1);
        assertUpdate(session, "INSERT INTO functions_partition(id, event_time, event_hour) VALUES (2, timestamp '2020-04-20 20:00:00', timestamp '2020-04-20 19:00:00')", 1);
        assertUpdate(session, "INSERT INTO functions_partition(id, event_time, event_hour) VALUES (3, timestamp '2020-04-20 20:00:00', timestamp '2020-04-20 20:00:00')", 1);
        assertUpdate(session, "INSERT INTO functions_partition(id, event_time, event_hour) VALUES (4, timestamp '2020-04-20 20:00:00', timestamp '2020-04-20 21:00:00')", 1);

        String query = "SELECT id FROM functions_partition WHERE event_time > date_trunc('HOUR', timestamp '2020-04-20 19:23:12')" +
                " AND event_time < date_trunc('MINUTE', timestamp '2020-04-20 20:59:59')";
        assertQuery(session, query, "VALUES (2), (3)");

        query = "SELECT id FROM functions_partition WHERE event_time > date_trunc('HOUR', timestamp '2020-04-20 18:00:00')" +
                " AND event_time < date_trunc('MINUTE', timestamp '2020-04-20 23:59:59')" +
                " AND date_trunc('HOUR', event_hour) >= timestamp '2020-04-20 19:00:00' AND date_trunc('HOUR', event_hour) <= timestamp '2020-04-20 20:00:00'";
        assertQuery(session, query, "VALUES (2), (3)");

        assertUpdate(session, "DROP TABLE functions_partition");
    }

    @Test
    public void testTruncateToHourFromSeconds()
    {
        createTableWithSpec(session, "truncate_partition_hour_from_sec", "{\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"source-name\": \"epoch_timestamp\",\n" +
                "      \"transform\": \"truncate[3600]\",\n" +
                "      \"name\": \"event_hour\"\n" +
                "    }\n" +
                "  ]\n" +
                "}", "event_hour", "INTEGER", Optional.of("epoch_timestamp"), Optional.of("INTEGER"));

        assertUpdate(session, "INSERT INTO truncate_partition_hour_from_sec(id, epoch_timestamp, event_hour) VALUES (1, 1587405601, 1587405600)", 1);
        assertUpdate(session, "INSERT INTO truncate_partition_hour_from_sec(id, epoch_timestamp, event_hour) VALUES (2, 1587409201, 1587409200)", 1);
        assertUpdate(session, "INSERT INTO truncate_partition_hour_from_sec(id, epoch_timestamp, event_hour) VALUES (3, 1587412801, 1587412800)", 1);
        assertUpdate(session, "INSERT INTO truncate_partition_hour_from_sec(id, epoch_timestamp, event_hour) VALUES (4, 1587416401, 1587416400)", 1);

        String query = "SELECT id FROM truncate_partition_hour_from_sec WHERE epoch_timestamp > 1587405601 AND epoch_timestamp < 1587416401";
        assertQuery(session, query, "VALUES (2), (3)");

        query = "SELECT id FROM truncate_partition_hour_from_sec WHERE epoch_timestamp > 1587405601 AND epoch_timestamp < 1587416401 AND " +
                "epoch_timestamp >= 1587409201 AND epoch_timestamp <= 1587412801";
        assertQuery(session, query, "VALUES (2), (3)");

        assertUpdate(session, "DROP TABLE truncate_partition_hour_from_sec");
    }

    @Test
    public void testTruncateToDayFromSeconds()
    {
        createTableWithSpec(session, "truncate_partition_day_from_sec", "{\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"source-name\": \"epoch_timestamp\",\n" +
                "      \"transform\": \"truncate[86400]\",\n" +
                "      \"name\": \"event_day\"\n" +
                "    }\n" +
                "  ]\n" +
                "}", "event_day", "INTEGER", Optional.of("epoch_timestamp"), Optional.of("INTEGER"));

        assertUpdate(session, "INSERT INTO truncate_partition_day_from_sec(id, epoch_timestamp, event_day) VALUES (1, 1587405601, 1587340800)", 1);
        assertUpdate(session, "INSERT INTO truncate_partition_day_from_sec(id, epoch_timestamp, event_day) VALUES (2, 1587409201, 1587340800)", 1);
        assertUpdate(session, "INSERT INTO truncate_partition_day_from_sec(id, epoch_timestamp, event_day) VALUES (3, 1587412801, 1587340800)", 1);
        assertUpdate(session, "INSERT INTO truncate_partition_day_from_sec(id, epoch_timestamp, event_day) VALUES (4, 1587416401, 1587340800)", 1);

        assertUpdate(session, "INSERT INTO truncate_partition_day_from_sec(id, epoch_timestamp, event_day) VALUES (5, 1587492001, 1587427200)", 1);
        assertUpdate(session, "INSERT INTO truncate_partition_day_from_sec(id, epoch_timestamp, event_day) VALUES (6, 1587495601, 1587427200)", 1);
        assertUpdate(session, "INSERT INTO truncate_partition_day_from_sec(id, epoch_timestamp, event_day) VALUES (7, 1587499201, 1587427200)", 1);
        assertUpdate(session, "INSERT INTO truncate_partition_day_from_sec(id, epoch_timestamp, event_day) VALUES (8, 1587502801, 1587427200)", 1);

        String query = "SELECT id FROM truncate_partition_day_from_sec WHERE epoch_timestamp > 1587405601 AND epoch_timestamp < 1587416401";
        assertQuery(session, query, "VALUES (2), (3)");

        query = "SELECT id FROM truncate_partition_day_from_sec WHERE epoch_timestamp > 1587405601 AND epoch_timestamp < 1587416401 AND " +
                "epoch_timestamp >= 1587409201 AND epoch_timestamp <= 1587412801";
        assertQuery(session, query, "VALUES (2), (3)");

        query = "SELECT id FROM truncate_partition_day_from_sec WHERE epoch_timestamp > 1587412801 AND epoch_timestamp < 1587495601";
        assertQuery(session, query, "VALUES (4), (5)");

        query = "SELECT id FROM truncate_partition_day_from_sec WHERE epoch_timestamp >= 1587502801";
        assertQuery(session, query, "VALUES (8)");
        assertUpdate(session, "DROP TABLE truncate_partition_day_from_sec");
    }

    @Test
    public void testTruncateToDayFromMilliseconds()
    {
        createTableWithSpec(session, "truncate_partition_day_from_ms", "{\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"source-name\": \"epoch_timestamp\",\n" +
                "      \"transform\": \"truncate[86400000]\",\n" +
                "      \"name\": \"event_day\"\n" +
                "    }\n" +
                "  ]\n" +
                "}", "event_day", "BIGINT", Optional.of("epoch_timestamp"), Optional.of("BIGINT"));

        assertUpdate(session, "INSERT INTO truncate_partition_day_from_ms(id, epoch_timestamp, event_day) VALUES (1, 1587405601000, 1587340800000)", 1);
        assertUpdate(session, "INSERT INTO truncate_partition_day_from_ms(id, epoch_timestamp, event_day) VALUES (2, 1587409201000, 1587340800000)", 1);
        assertUpdate(session, "INSERT INTO truncate_partition_day_from_ms(id, epoch_timestamp, event_day) VALUES (3, 1587412801000, 1587340800000)", 1);
        assertUpdate(session, "INSERT INTO truncate_partition_day_from_ms(id, epoch_timestamp, event_day) VALUES (4, 1587416401000, 1587340800000)", 1);

        assertUpdate(session, "INSERT INTO truncate_partition_day_from_ms(id, epoch_timestamp, event_day) VALUES (5, 1587492001000, 1587427200000)", 1);
        assertUpdate(session, "INSERT INTO truncate_partition_day_from_ms(id, epoch_timestamp, event_day) VALUES (6, 1587495601000, 1587427200000)", 1);
        assertUpdate(session, "INSERT INTO truncate_partition_day_from_ms(id, epoch_timestamp, event_day) VALUES (7, 1587499201000, 1587427200000)", 1);
        assertUpdate(session, "INSERT INTO truncate_partition_day_from_ms(id, epoch_timestamp, event_day) VALUES (8, 1587502801000, 1587427200000)", 1);

        String query = "SELECT id FROM truncate_partition_day_from_ms WHERE epoch_timestamp > 1587405601000 AND epoch_timestamp < 1587416401000";
        assertQuery(session, query, "VALUES (2), (3)");

        query = "SELECT id FROM truncate_partition_day_from_ms WHERE epoch_timestamp > 1587405601000 AND epoch_timestamp < 1587416401000 AND " +
                "epoch_timestamp >= 1587409201000 AND epoch_timestamp <= 1587412801000";
        assertQuery(session, query, "VALUES (2), (3)");

        query = "SELECT id FROM truncate_partition_day_from_ms WHERE epoch_timestamp > 1587412801000 AND epoch_timestamp < 1587495601000";
        assertQuery(session, query, "VALUES (4), (5)");

        query = "SELECT id FROM truncate_partition_day_from_ms WHERE epoch_timestamp >= 1587502801000";
        assertQuery(session, query, "VALUES (8)");
        assertUpdate(session, "DROP TABLE truncate_partition_day_from_ms");
    }

    @Test
    public void testQueryThroughView()
    {
        createTableHourSpec(session, "queryThroughView");
        assertUpdate(session, "CREATE VIEW queryThroughView_view AS SELECT * FROM queryThroughView WHERE event_hour >= timestamp '2020-04-20 19:00:00'");

        assertUpdate(session, "INSERT INTO queryThroughView(id, event_time, event_hour) VALUES (1, timestamp '2020-04-20 20:00:00', timestamp '2020-04-20 18:00:00')", 1);
        assertUpdate(session, "INSERT INTO queryThroughView(id, event_time, event_hour) VALUES (2, timestamp '2020-04-20 20:00:00', timestamp '2020-04-20 19:00:00')", 1);
        assertUpdate(session, "INSERT INTO queryThroughView(id, event_time, event_hour) VALUES (3, timestamp '2020-04-20 20:00:00', timestamp '2020-04-20 20:00:00')", 1);
        assertUpdate(session, "INSERT INTO queryThroughView(id, event_time, event_hour) VALUES (4, timestamp '2020-04-20 20:00:00', timestamp '2020-04-20 21:00:00')", 1);

        String query = "SELECT id FROM queryThroughView_view WHERE event_hour >= timestamp '2020-04-20 20:00:00' AND event_hour < timestamp '2020-04-20 21:00:00'";
        assertQuery(session, query, "SELECT 3");

        query = "SELECT id FROM queryThroughView_view WHERE event_time >= timestamp '2020-04-20 20:00:00' AND event_time < timestamp '2020-04-20 21:00:00'";
        assertQuery(session, query, "SELECT 3");

        assertUpdate(session, "DROP VIEW queryThroughView_view");
        assertUpdate(session, "DROP TABLE queryThroughView");
    }

    @Test
    public void testNullPartitionValue()
    {
        createTableDaySpec(session, "nullPartitionValue");

        assertUpdate(session, "INSERT INTO nullPartitionValue(id, event_time, event_date) VALUES (1, timestamp '2020-04-19 00:00:00', date '2020-04-18')", 1);
        assertUpdate(session, "INSERT INTO nullPartitionValue(id, event_time, event_date) VALUES (2, timestamp '2020-04-18 11:00:00', date '2020-04-19')", 1);
        assertUpdate(session, "INSERT INTO nullPartitionValue(id, event_time, event_date) VALUES (3, timestamp '2020-04-20 00:00:00', null)", 1);
        assertUpdate(session, "INSERT INTO nullPartitionValue(id, event_time, event_date) VALUES (4, timestamp '2020-04-20 00:00:00', date '2020-04-21')", 1);

        String query = "SELECT id FROM nullPartitionValue WHERE event_time > timestamp '2020-04-18 00:00:00' AND event_time <= timestamp '2020-04-20 23:59:59'";
        assertQuery(session, query, "VALUES (1), (2)");

        assertUpdate(session, "DROP TABLE nullPartitionValue");
    }

    private void createTableHourSpec(Session session, String tableName)
    {
        createTableWithSpec(session, tableName, HOUR_SPEC, "event_hour", "timestamp", Optional.empty(), Optional.empty());
    }

    private void createTableDaySpec(Session session, String tableName)
    {
        createTableWithSpec(session, tableName, DAY_SPEC, "event_date", "date", Optional.empty(), Optional.empty());
    }

    private void createTableWithSpec(Session session, String tableName, String partitionSpec, String partitionColName, String partitionColType, Optional<String> additionalColName, Optional<String> additionalColType)
    {
        String additionalCol = (additionalColName.isPresent() && additionalColType.isPresent()) ? String.format("%s %s,", additionalColName.get(), additionalColType.get()) : "";
        assertUpdate(
                session,
                "CREATE TABLE " + tableName + "(\n"
                        + "id integer,\n"
                        + "event_time timestamp,"
                        + additionalCol
                        + partitionColName + " " + partitionColType + ")"
                        + "WITH (format='ORC', partitioned_by = ARRAY['" + partitionColName
                        + "'], partition_spec = '" + partitionSpec + "')");
    }

    private Session getHiveSession()
    {
        return testSessionBuilder()
                .setCatalog(HIVE_CATALOG)
                .setSchema(TPCH_SCHEMA)
                .setIdentity(Identity.forUser(HIVE_CATALOG)
                        .withRole(HIVE_CATALOG, new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .build();
    }
}
