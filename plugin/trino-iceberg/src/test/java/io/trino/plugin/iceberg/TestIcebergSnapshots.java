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
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class TestIcebergSnapshots
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

        assertQuery(format("SELECT count(*) FROM \"%s@%d\"", tableName, ids.get(0)), "VALUES(0)");
        assertQuery(format("SELECT * FROM \"%s@%d\"", tableName, ids.get(1)), "VALUES(1,1)");
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
}
