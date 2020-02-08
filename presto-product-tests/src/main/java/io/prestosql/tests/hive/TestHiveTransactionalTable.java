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
package io.prestosql.tests.hive;

import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.HIVE_TRANSACTIONAL;
import static io.prestosql.tests.TestGroups.STORAGE_FORMATS;
import static io.prestosql.tests.utils.QueryExecutors.onHive;

public class TestHiveTransactionalTable
        extends HiveProductTest
{
    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL}, dataProvider = "isTablePartitioned")
    public void testReadFullAcid(boolean isPartitioned)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Presto Hive transactional tables are supported with Hive version 3 or above");
        }

        String tableName = "test_full_acid_table_read";
        onHive().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onHive().executeQuery("CREATE TABLE " + tableName + " (col INT, fcol INT) " +
                (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                "STORED AS ORC " +
                "TBLPROPERTIES ('transactional'='true') ");
        try {
            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";
            onHive().executeQuery("INSERT OVERWRITE TABLE " + tableName + hivePartitionString + " VALUES (21, 1)");

            String selectFromOnePartitionsSql = "SELECT col, fcol FROM " + tableName + " ORDER BY col";
            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(21, 1));

            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (22, 2)");
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(21, 1), row(22, 2));

            // test filtering
            assertThat(query("SELECT col, fcol FROM " + tableName + " WHERE fcol = 1 ORDER BY col")).containsOnly(row(21, 1));

            // delete a row
            onHive().executeQuery("DELETE FROM " + tableName + " WHERE fcol=2");
            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(21, 1));

            // update the existing row
            String predicate = "fcol = 1" + (isPartitioned ? " AND part_col = 2 " : "");
            onHive().executeQuery("UPDATE " + tableName + " SET col = 23 WHERE " + predicate);
            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(23, 1));
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL}, dataProvider = "isTablePartitioned")
    public void testReadInsertOnly(boolean isPartitioned)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Presto Hive transactional tables are supported with Hive version 3 or above");
        }

        String tableName = "test_insert_only_table_read";
        onHive().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onHive().executeQuery("CREATE TABLE " + tableName + " (col INT) " +
                (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                "STORED AS ORC " +
                "TBLPROPERTIES ('transactional_properties'='insert_only', 'transactional'='true') ");
        try {
            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";
            String predicate = isPartitioned ? " WHERE part_col = 2 " : "";

            onHive().executeQuery("INSERT OVERWRITE TABLE " + tableName + hivePartitionString + " SELECT 1");
            String selectFromOnePartitionsSql = "SELECT col FROM " + tableName + predicate + " ORDER BY COL";
            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(1));

            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " SELECT 2");
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(1), row(2));

            onHive().executeQuery("INSERT OVERWRITE TABLE " + tableName + hivePartitionString + " SELECT 3");
            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(3));
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }


    @DataProvider
    public Object[][] isTablePartitioned()
    {
        return new Object[][] {
                {true},
                {false}
        };
    }
}
