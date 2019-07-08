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

public class TestInsertOnlyAcidTableRead
        extends HiveProductTest
{
    @Test(dataProvider = "isTablePartitioned", groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL})
    public void testSelectFromInsertOnlyAcidTable(boolean isPartitioned)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Presto Hive transactional tables are supported with Hive version 3 or above");
        }

        String tableName = "insert_only_acid_table" + (isPartitioned ? "_partitioned" : "");

        String createTable = "CREATE TABLE IF NOT EXISTS " +
                tableName +
                " (col INT) " +
                (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                "STORED AS ORC " +
                "TBLPROPERTIES ('transactional_properties'='insert_only', 'transactional'='true') ";

        onHive().executeQuery(createTable);

        try {
            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";
            String predicate = isPartitioned ? " WHERE part_col = 2 " : "";

            onHive().executeQuery(
                    "INSERT OVERWRITE TABLE " + tableName + hivePartitionString + " select 1");

            String selectFromOnePartitionsSql = "SELECT col FROM " + tableName + predicate + " ORDER BY COL";
            QueryResult onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsOnly(row(1));

            onHive().executeQuery(
                    "INSERT INTO TABLE " + tableName + hivePartitionString + " select 2");
            onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsExactly(row(1), row(2));

            onHive().executeQuery(
                    "INSERT OVERWRITE TABLE " + tableName + hivePartitionString + " select 3");
            onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsOnly(row(3));
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
