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

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Iterables.getLast;
import static io.trino.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLegacySyntaxForIcebergSnapshotAccess
        extends AbstractTestQueryFramework
{
    private static final int ID_FIELD = 0;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner();
    }

    @Test
    public void testReadingFromSpecificSnapshot()
    {
        String tableName = "test_reading_snapshot" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE %s (a bigint, b bigint)", tableName));
        assertUpdate(format("INSERT INTO %s VALUES(1, 1)", tableName), 1);
        List<Long> ids = getSnapshotsIdsByCreationOrder(tableName);

        assertQuery(sessionWithLegacySyntaxSupport(), format("SELECT count(*) FROM \"%s@%d\"", tableName, ids.get(0)), "VALUES(0)");
        assertQuery(sessionWithLegacySyntaxSupport(), format("SELECT * FROM \"%s@%d\"", tableName, ids.get(1)), "VALUES(1,1)");
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testLegacySnapshotSyntaxSupport()
    {
        String tableName = "test_legacy_snapshot_access" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE %s (a BIGINT, b BIGINT)", tableName));
        assertUpdate(format("INSERT INTO %s VALUES(1, 1)", tableName), 1);
        List<Long> ids = getSnapshotsIdsByCreationOrder(tableName);
        // come up with a timestamp value in future that is not an already existing id
        long futureTimeStamp = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5);
        while (ids.contains(futureTimeStamp)) {
            futureTimeStamp += TimeUnit.MINUTES.toMillis(5);
        }

        String selectAllFromFutureTimeStamp = format("SELECT * FROM \"%s@%d\"", tableName, futureTimeStamp);
        String selectAllFromLatestId = format("SELECT * FROM \"%s@%d\"", tableName, getLast(ids));
        String selectFromPartitionsTable = format("SELECT record_count FROM \"%s$partitions@%d\"", tableName, getLast(ids));

        assertQuery(sessionWithLegacySyntaxSupport(), selectAllFromFutureTimeStamp, "VALUES(1, 1)");
        assertQuery(sessionWithLegacySyntaxSupport(), selectAllFromLatestId, "VALUES(1, 1)");
        assertQuery(sessionWithLegacySyntaxSupport(), selectFromPartitionsTable, "VALUES(1)");

        // DISABLED
        String errorMessage = "Failed to access snapshot .* for table .*. This syntax for accessing Iceberg tables is not "
                + "supported. Use the AS OF syntax OR set the catalog session property "
                + "allow_legacy_snapshot_syntax=true for temporarily restoring previous behavior.";
        assertThatThrownBy(() -> query(getSession(), selectAllFromFutureTimeStamp))
                .hasMessageMatching(errorMessage);
        assertThatThrownBy(() -> query(getSession(), selectAllFromLatestId))
                .hasMessageMatching(errorMessage);
        assertThatThrownBy(() -> query(getSession(), selectFromPartitionsTable))
                .hasMessageMatching(errorMessage);

        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testSelectWithMoreThanOneSnapshotOfTheSameTable()
    {
        String tableName = "test_reading_snapshot" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE %s (a bigint, b bigint)", tableName));
        assertUpdate(format("INSERT INTO %s VALUES(1, 1)", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES(2, 2)", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES(3, 3)", tableName), 1);
        List<Long> ids = getSnapshotsIdsByCreationOrder(tableName);

        assertQuery(format("SELECT * FROM %s", tableName), "SELECT * FROM (VALUES(1,1), (2,2), (3,3))");
        assertQuery(
                sessionWithLegacySyntaxSupport(),
                format("SELECT * FROM %1$s EXCEPT (SELECT * FROM \"%1$s@%2$d\" EXCEPT SELECT * FROM \"%1$s@%3$d\")", tableName, ids.get(2), ids.get(1)),
                "SELECT * FROM (VALUES(1,1), (3,3))");
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    private List<Long> getSnapshotsIdsByCreationOrder(String tableName)
    {
        return getQueryRunner().execute(
                format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at", tableName))
                .getMaterializedRows().stream()
                .map(row -> (Long) row.getField(ID_FIELD))
                .collect(toList());
    }

    private Session sessionWithLegacySyntaxSupport()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "allow_legacy_snapshot_syntax", "true")
                .build();
    }
}
