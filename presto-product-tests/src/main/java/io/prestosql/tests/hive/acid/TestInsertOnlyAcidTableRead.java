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

import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.query.QueryResult;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.STORAGE_FORMATS;
import static io.prestosql.tests.hive.acid.HiveHelper.canRunAcidTests;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static java.util.Locale.ENGLISH;

public class TestInsertOnlyAcidTableRead
        extends ProductTest
{
    @Test(groups = {STORAGE_FORMATS}, dataProvider = "insertOnlyAcidTableTypes")
    public void testSelectFromInsertOnlyAcidTable(InsertOnlyAcidTableType type)
            throws SQLException
    {
        if (!canRunAcidTests()) {
            return;
        }

        String tableName = "insert_only_acid_table" + type.name().toLowerCase(ENGLISH);
        createTable(tableName, type.isPartitioned());

        try {
            onHive().executeQuery(
                    "INSERT OVERWRITE TABLE " + tableName + getHivePartitionString(type.isPartitioned()) + " select 1");

            String selectFromOnePartitionsSql = "SELECT col FROM " + tableName + getPrestoPredicate(type.isPartitioned()) + " ORDER BY COL";
            QueryResult onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsOnly(row(1));

            onHive().executeQuery(
                    "INSERT INTO TABLE " + tableName + getHivePartitionString(type.isPartitioned()) + " select 2");
            onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsExactly(row(1), row(2));

            onHive().executeQuery(
                    "INSERT OVERWRITE TABLE " + tableName + getHivePartitionString(type.isPartitioned()) + " select 3");
            onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsOnly(row(3));
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }

    private String getHivePartitionString(boolean isPartitioned)
    {
        if (!isPartitioned) {
            return "";
        }

        return " PARTITION (part_col=2) ";
    }

    private String getPrestoPredicate(boolean isPartitioned)
    {
        if (!isPartitioned) {
            return "";
        }

        return " WHERE part_col = 2 ";
    }

    private void createTable(String tableName, boolean isPartitioned)
    {
        StringBuilder builder = new StringBuilder()
                .append("CREATE TABLE IF NOT EXISTS ")
                .append(tableName)
                .append(" (col INT) ");
        if (isPartitioned) {
            builder.append("PARTITIONED BY (part_col INT) ");
        }

        builder.append("STORED AS ORC ")
                .append("TBLPROPERTIES ('transactional_properties'='insert_only', 'transactional'='true') ");

        onHive().executeQuery(builder.toString());
    }

    @DataProvider
    public Object[][] insertOnlyAcidTableTypes()
    {
        return new Object[][] {
                {InsertOnlyAcidTableType.UNPARTITIONED},
                {InsertOnlyAcidTableType.PARTITIONED}
        };
    }

    private enum InsertOnlyAcidTableType
    {
        UNPARTITIONED {
            @Override
            boolean isPartitioned()
            {
                return false;
            }
        },
        PARTITIONED {
            @Override
            boolean isPartitioned()
            {
                return true;
            }
        },
        /**/;

        abstract boolean isPartitioned();
    }
}
