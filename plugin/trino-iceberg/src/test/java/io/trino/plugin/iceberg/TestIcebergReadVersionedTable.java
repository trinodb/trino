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
package io.trino.plugin.iceberg;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static io.trino.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;

public class TestIcebergReadVersionedTable
        extends AbstractTestQueryFramework
{
    private long v1SnapshotId;
    private long v1EpochMillis;
    private long v2SnapshotId;
    private long v2EpochMillis;
    private long incorrectSnapshotId;

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner();
    }

    @BeforeClass
    public void setUp()
            throws InterruptedException
    {
        assertQuerySucceeds("CREATE TABLE test_iceberg_read_versioned_table(a_string varchar, an_integer integer)");
        assertQuerySucceeds("INSERT INTO test_iceberg_read_versioned_table VALUES ('a', 1)");
        v1SnapshotId = getLatestSnapshotId("test_iceberg_read_versioned_table");
        v1EpochMillis = getCommittedAtInEpochMilliSeconds("test_iceberg_read_versioned_table", v1SnapshotId);
        Thread.sleep(1);
        assertQuerySucceeds("INSERT INTO test_iceberg_read_versioned_table VALUES ('b', 2)");
        v2SnapshotId = getLatestSnapshotId("test_iceberg_read_versioned_table");
        v2EpochMillis = getCommittedAtInEpochMilliSeconds("test_iceberg_read_versioned_table", v2SnapshotId);
        incorrectSnapshotId = v2SnapshotId + 1;
    }

    @Test
    public void testSelectTableWithEndSnapshotId()
    {
        assertQuery("SELECT * FROM test_iceberg_read_versioned_table FOR VERSION AS OF " + v1SnapshotId, "VALUES ('a', 1)");
        assertQuery("SELECT * FROM test_iceberg_read_versioned_table FOR VERSION AS OF " + v2SnapshotId, "VALUES ('a', 1), ('b', 2)");
        assertQueryFails("SELECT * FROM test_iceberg_read_versioned_table FOR VERSION AS OF " + incorrectSnapshotId, "Iceberg snapshot ID does not exists: " + incorrectSnapshotId);
    }

    @Test
    public void testSelectTableWithEndShortTimestampWithTimezone()
    {
        assertQueryFails("SELECT * FROM test_iceberg_read_versioned_table FOR TIMESTAMP AS OF TIMESTAMP '1970-01-01 00:00:00.001000000 Z'",
                "\\QNo version history table tpch.\"test_iceberg_read_versioned_table\" at or before 1970-01-01T00:00:00.001Z");
        assertQuery("SELECT * FROM test_iceberg_read_versioned_table FOR TIMESTAMP AS OF " + timestampLiteral(v1EpochMillis, 9), "VALUES ('a', 1)");
        assertQuery("SELECT * FROM test_iceberg_read_versioned_table FOR TIMESTAMP AS OF " + timestampLiteral(v2EpochMillis, 9), "VALUES ('a', 1), ('b', 2)");
    }

    @Test
    public void testSelectTableWithEndLongTimestampWithTimezone()
    {
        assertQueryFails("SELECT * FROM test_iceberg_read_versioned_table FOR TIMESTAMP AS OF TIMESTAMP '1970-01-01 00:00:00.001000000 Z'",
                "\\QNo version history table tpch.\"test_iceberg_read_versioned_table\" at or before 1970-01-01T00:00:00.001Z");
        assertQuery("SELECT * FROM test_iceberg_read_versioned_table FOR TIMESTAMP AS OF " + timestampLiteral(v1EpochMillis, 9), "VALUES ('a', 1)");
        assertQuery("SELECT * FROM test_iceberg_read_versioned_table FOR TIMESTAMP AS OF " + timestampLiteral(v2EpochMillis, 9), "VALUES ('a', 1), ('b', 2)");
    }

    @Test
    public void testEndVersionInTableNameAndForClauseShouldFail()
    {
        assertQueryFails("SELECT * FROM \"test_iceberg_read_versioned_table@" + v1SnapshotId + "\" FOR VERSION AS OF " + v1SnapshotId,
                "Invalid Iceberg table name: test_iceberg_read_versioned_table@%d".formatted(v1SnapshotId));

        assertQueryFails("SELECT * FROM \"test_iceberg_read_versioned_table@" + v1SnapshotId + "\" FOR TIMESTAMP AS OF " + timestampLiteral(v1EpochMillis, 9),
                "Invalid Iceberg table name: test_iceberg_read_versioned_table@%d".formatted(v1SnapshotId));
    }

    @Test
    public void testSystemTables()
    {
        // TODO https://github.com/trinodb/trino/issues/12920
        assertQueryFails("SELECT * FROM \"test_iceberg_read_versioned_table$partitions\" FOR VERSION AS OF " + v1SnapshotId,
                "This connector does not support versioned tables");
    }

    private long getLatestSnapshotId(String tableName)
    {
        return (long) computeScalar(format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES", tableName));
    }

    private long getCommittedAtInEpochMilliSeconds(String tableName, long snapshotId)
    {
        return ((ZonedDateTime) computeScalar(format("SELECT committed_at FROM \"%s$snapshots\" WHERE snapshot_id=%s", tableName, snapshotId)))
                .toInstant().toEpochMilli();
    }

    private static String timestampLiteral(long epochMilliSeconds, int precision)
    {
        return DateTimeFormatter.ofPattern("'TIMESTAMP '''uuuu-MM-dd HH:mm:ss." + "S".repeat(precision) + " VV''")
                .format(Instant.ofEpochMilli(epochMilliSeconds).atZone(UTC));
    }
}
