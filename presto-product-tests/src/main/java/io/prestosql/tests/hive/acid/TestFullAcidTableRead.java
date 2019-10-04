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

public class TestFullAcidTableRead
        extends ProductTest
{
    @Test(groups = {STORAGE_FORMATS}, dataProvider = "fullAcidTableTypes")
    public void testSelectFromFullAcidTable(FullAcidTableType type)
            throws SQLException
    {
        if (!canRunAcidTests()) {
            return;
        }

        String tableName = "full_acid_table" + type.name().toLowerCase(ENGLISH);
        createTable(tableName, type.isPartitioned());

        try {
            onHive().executeQuery("INSERT OVERWRITE TABLE " + tableName + getHivePartitionString(type.isPartitioned()) + " VALUES (21, 1)");

            String selectFromOnePartitionsSql = "SELECT col, fcol FROM " + tableName + " ORDER BY col";
            QueryResult onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsOnly(row(21, 1));

            onHive().executeQuery("INSERT INTO TABLE " + tableName + getHivePartitionString(type.isPartitioned()) + " VALUES (22, 2)");
            onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsExactly(row(21, 1), row(22, 2));

            // test filtering
            onePartitionQueryResult = query("SELECT col, fcol FROM " + tableName + " WHERE fcol = 1 ORDER BY col");
            assertThat(onePartitionQueryResult).containsOnly(row(21, 1));

            // delete a row
            onHive().executeQuery(
                    "DELETE FROM " + tableName + " where fcol=2");
            onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsOnly(row(21, 1));

            // update the existing row
            onHive().executeQuery(
                    "UPDATE " + tableName + " set col = 23 " + getPrestoPartitionPredicate(type.isPartitioned(), "fcol = 1"));
            onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsOnly(row(23, 1));
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

    private String getPrestoPartitionPredicate(boolean isPartitioned, String columnPredicate)
    {
        String predicate = " WHERE " + columnPredicate;
        if (isPartitioned) {
            predicate += " AND part_col = 2 ";
        }
        return predicate;
    }

    private void createTable(String tableName, boolean isPartitioned)
    {
        StringBuilder builder = new StringBuilder()
                .append("CREATE TABLE IF NOT EXISTS ")
                .append(tableName)
                .append(" (col INT,")
                .append("fcol INT) ");
        if (isPartitioned) {
            builder.append("PARTITIONED BY (part_col INT) ");
        }

        builder.append("STORED AS ORC ")
                .append("TBLPROPERTIES ('transactional'='true') ");

        onHive().executeQuery(builder.toString());
    }

    @DataProvider
    public Object[][] fullAcidTableTypes()
    {
        return new Object[][] {
                {FullAcidTableType.UNPARTITIONED},
                {FullAcidTableType.PARTITIONED}
        };
    }

    private enum FullAcidTableType
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
