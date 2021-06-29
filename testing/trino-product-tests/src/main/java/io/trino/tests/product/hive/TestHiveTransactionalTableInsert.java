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
package io.trino.tests.product.hive;

import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.product.TestGroups.HIVE_TRANSACTIONAL;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;

public class TestHiveTransactionalTableInsert
        extends HiveProductTest
{
    @Test(dataProvider = "transactionalTableType", groups = HIVE_TRANSACTIONAL)
    public void testInsertIntoTransactionalTable(TransactionalTableType type)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Hive transactional tables are supported with Hive version 3 or above");
        }

        String tableName = "test_insert_into_transactional_table_" + type.name().toLowerCase(ENGLISH);
        onHive().executeQuery("" +
                "CREATE TABLE " + tableName + "(a bigint)" +
                "CLUSTERED BY(a) INTO 4 BUCKETS STORED AS ORC " +
                hiveTableProperties(type));

        try {
            query("INSERT INTO " + tableName + " (a) VALUES (42)");
            assertThat(query("SELECT * FROM " + tableName))
                    .containsOnly(row(42));
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }

    @DataProvider
    public Object[][] transactionalTableType()
    {
        return new Object[][] {
                {TransactionalTableType.ACID},
                {TransactionalTableType.INSERT_ONLY},
        };
    }

    private static String hiveTableProperties(TransactionalTableType transactionalTableType)
    {
        return transactionalTableType.getHiveTableProperties().stream()
                .collect(joining(",", "TBLPROPERTIES (", ")"));
    }
}
