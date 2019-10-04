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
package io.prestosql.tests.hive.acid;

import io.prestosql.tempto.query.QueryResult;
import io.prestosql.tests.hive.HiveProductTest;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.HIVE_TRANSACTIONAL;
import static io.prestosql.tests.TestGroups.STORAGE_FORMATS;
import static io.prestosql.tests.utils.QueryExecutors.onHive;

public class TestFullAcidTableRead
        extends HiveProductTest
{
    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL}, dataProvider = "isTablePartitioned")
    public void testRead(boolean isPartitioned)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Presto Hive transactional tables are supported with Hive version 3 or above");
        }

        String tableName = isPartitioned ? "full_acid_read_partitioned" : "full_acid_read";
        createTable(tableName, isPartitioned);

        try {
            onHive().executeQuery("INSERT OVERWRITE TABLE " + tableName + getHivePartitionString(isPartitioned) + " VALUES (21, 1)");

            String selectFromOnePartitionsSql = "SELECT col, fcol FROM " + tableName + " ORDER BY col";
            QueryResult onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsOnly(row(21, 1));

            onHive().executeQuery("INSERT INTO TABLE " + tableName + getHivePartitionString(isPartitioned) + " VALUES (22, 2)");
            onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsExactly(row(21, 1), row(22, 2));

            // test filtering
            onePartitionQueryResult = query("SELECT col, fcol FROM " + tableName + " WHERE fcol = 1 ORDER BY col");
            assertThat(onePartitionQueryResult).containsOnly(row(21, 1));

            // delete a row
            onHive().executeQuery("DELETE FROM " + tableName + " WHERE fcol=2");
            onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsOnly(row(21, 1));

            // update the existing row
            onHive().executeQuery("UPDATE " + tableName + " SET col = 23 " + addPartitionPredicate(isPartitioned, "fcol = 1"));
            onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsOnly(row(23, 1));
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }

    private static String getHivePartitionString(boolean isPartitioned)
    {
        if (!isPartitioned) {
            return "";
        }

        return " PARTITION (part_col=2) ";
    }

    private static String addPartitionPredicate(boolean isPartitioned, String columnPredicate)
    {
        String predicate = " WHERE " + columnPredicate;
        if (isPartitioned) {
            predicate += " AND part_col = 2 ";
        }
        return predicate;
    }

    private static void createTable(String tableName, boolean isPartitioned)
    {
        onHive().executeQuery("CREATE TABLE IF NOT EXISTS " + tableName + " (col INT, fcol INT) " +
                (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                "STORED AS ORC " +
                "TBLPROPERTIES ('transactional'='true') ");
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
